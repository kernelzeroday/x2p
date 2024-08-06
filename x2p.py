import logging,argparse,json,os,asyncio,aiofiles,pytest,csv,tracemalloc
from typing import Dict,Any,List,Union
from xlcalculator import ModelCompiler,Model,Evaluator
from colorama import init,Fore
init(autoreset=True)
LOG_TO_FILE,LOG_CONSOLE,LOG_JSON,LOG_DISK=False,True,True,False
logging.basicConfig(level=logging.DEBUG)
logger=logging.getLogger(__name__)
def setup_logging(log_file:str):
    logger.addHandler(logging.StreamHandler())
    file_handler=logging.FileHandler(log_file)if LOG_TO_FILE else None
    (file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))if file_handler else None)
    logger.addHandler(file_handler)if file_handler else None
async def log_info(message:str):
    if LOG_CONSOLE:print(Fore.GREEN+message)
    if LOG_TO_FILE:
        async with aiofiles.open("xls_analysis.txt","a")as f:await f.write(message+"\n")
    if LOG_JSON:
        async with aiofiles.open("xls_analysis.json","a")as f:await f.write(json.dumps({"level":"INFO","message":message})+"\n")
    if LOG_DISK:
        async with aiofiles.open("xls_analysis.log","a")as f:await f.write(f"INFO: {message}\n")
async def log_error(message:str):
    if LOG_CONSOLE:print(Fore.RED+message)
    if LOG_TO_FILE:
        async with aiofiles.open("xls_analysis.txt","a")as f:await f.write("ERROR: "+message+"\n")
    if LOG_JSON:
        async with aiofiles.open("xls_analysis.json","a")as f:await f.write(json.dumps({"level":"ERROR","message":message})+"\n")
    if LOG_DISK:
        async with aiofiles.open("xls_analysis.log","a")as f:await f.write(f"ERROR: {message}\n")
async def save_json(data:Dict[str,Any],filename:str):
    if data:
        async with aiofiles.open(filename,"w")as f:await f.write(json.dumps(data,indent=2))
        async with aiofiles.open(filename.replace('.json','.raw.json'),"w")as f:await f.write(json.dumps(data))
async def save_csv(data:Dict[str,Any],filename:str):
    if data:
        async with aiofiles.open(filename,"w",newline='')as f:
            writer=csv.writer(f)
            for sheet,sheet_data in data.items():
                await f.write(f"{sheet}\n")
                for address,value in sheet_data.items():
                    await f.write(f"{address},{value}\n")
                await f.write("\n")
async def save_text(data:Dict[str,Any],filename:str):
    if data:
        async with aiofiles.open(filename,"w")as f:
            for sheet,sheet_data in data.items():
                await f.write(f"{sheet}:\n")
                for address,value in sheet_data.items():
                    await f.write(f"  {address}: {value}\n")
                await f.write("\n")
        async with aiofiles.open(filename.replace('.txt','.raw.txt'),"w")as f:
            for sheet,sheet_data in data.items():
                await f.write(f"{sheet}\n")
                for address,value in sheet_data.items():
                    await f.write(f"{address},{value}\n")
                await f.write("\n")
def process_cell(cell:Union[Any,List[Any]])->List[str]:
    return [cell.address]if hasattr(cell,"address")else([c.address for c in cell if hasattr(c,"address")]if isinstance(cell,list)else[])
def get_cell_value(cell:Any)->Any:
    return cell.value if hasattr(cell,"value")else cell.formula if hasattr(cell,"formula")else str(cell)
async def analyze_xls(filename:str,output_prefix:str):
    try:
        compiler=ModelCompiler()
        new_model=compiler.read_and_parse_archive(filename,build_code=True)
        evaluator=Evaluator(new_model)
        results,formulas,structure={},{},{}
        for sheet in new_model.cells:
            sheet_results,sheet_formulas,sheet_structure={},{},{}
            sheet_data=new_model.cells[sheet]
            if isinstance(sheet_data,dict):
                for cell_key,cell_value in sheet_data.items():
                    for address in process_cell(cell_value):
                        try:
                            value=evaluator.evaluate(f"{sheet}!{address}")
                            if value is not None and value!="":
                                sheet_results[address]=str(value)
                                formula=get_cell_value(cell_value)
                                if isinstance(formula,str) and formula.startswith('='):
                                    sheet_formulas[address]=formula
                                await log_info(f"Evaluated {sheet}!{address}: {value}")
                                row,col=int(address[1:]),ord(address[0])-65
                                if row>1:
                                    above_address=f"{chr(col+65)}{row-1}"
                                    if above_address in sheet_results:
                                        sheet_structure[address]={"description":sheet_results[above_address],"value":value}
                        except Exception as e:await log_error(f"Error evaluating {sheet}!{address}: {str(e)}")
            else:
                try:
                    for row_idx,row in enumerate(sheet_data):
                        for col_idx,cell in enumerate(row):
                            address=f"{chr(65+col_idx)}{row_idx+1}"
                            value=get_cell_value(cell)
                            if value is not None and value!="":
                                sheet_results[address]=str(value)
                                if isinstance(value,str) and value.startswith('='):
                                    sheet_formulas[address]=value
                                await log_info(f"Processed {sheet}!{address}: {value}")
                                if row_idx>0:
                                    above_address=f"{chr(65+col_idx)}{row_idx}"
                                    if above_address in sheet_results:
                                        sheet_structure[address]={"description":sheet_results[above_address],"value":value}
                except TypeError:
                    if hasattr(sheet_data,"address"):
                        address,value=sheet_data.address,get_cell_value(sheet_data)
                        if value is not None and value!="":
                            sheet_results[address]=str(value)
                            if isinstance(value,str) and value.startswith('='):
                                sheet_formulas[address]=value
                            await log_info(f"Processed single cell {sheet}!{address}: {value}")
            if sheet_results:results[sheet]=sheet_results
            if sheet_formulas:formulas[sheet]=sheet_formulas
            if sheet_structure:structure[sheet]=sheet_structure
        if results:
            await save_json(results,f"{output_prefix}_results.json")
            await save_csv(results,f"{output_prefix}_results.csv")
            await save_text(results,f"{output_prefix}_results.txt")
            await log_info(f"Results saved to {output_prefix}_results.json, {output_prefix}_results.csv, {output_prefix}_results.txt")
        if formulas:
            await save_json(formulas,f"{output_prefix}_formulas.json")
            await save_csv(formulas,f"{output_prefix}_formulas.csv")
            await save_text(formulas,f"{output_prefix}_formulas.txt")
            await log_info(f"Formulas saved to {output_prefix}_formulas.json, {output_prefix}_formulas.csv, {output_prefix}_formulas.txt")
        if structure:
            await save_json(structure,f"{output_prefix}_structure.json")
            await save_csv(structure,f"{output_prefix}_structure.csv")
            await save_text(structure,f"{output_prefix}_structure.txt")
            await log_info(f"Structure saved to {output_prefix}_structure.json, {output_prefix}_structure.csv, {output_prefix}_structure.txt")
    except Exception as e:await log_error(f"Error analyzing XLS file: {str(e)}")
@pytest.mark.asyncio
async def test_analyze_xls(tmp_path):
    test_file=tmp_path/"test.xlsx"
    output_prefix=tmp_path/"output"
    await analyze_xls(str(test_file),str(output_prefix))
    assert (tmp_path/"output_results.json").exists()
    assert (tmp_path/"output_results.csv").exists()
    assert (tmp_path/"output_results.txt").exists()
    assert (tmp_path/"output_formulas.json").exists()
    assert (tmp_path/"output_formulas.csv").exists()
    assert (tmp_path/"output_formulas.txt").exists()
    assert (tmp_path/"output_structure.json").exists()
    assert (tmp_path/"output_structure.csv").exists()
    assert (tmp_path/"output_structure.txt").exists()
async def main():
    parser=argparse.ArgumentParser(description="Analyze XLS file and extract calculations, formulas, and structure")
    parser.add_argument("filename",type=str,help="Path to XLS file")
    parser.add_argument("-o","--output-prefix",type=str,default="xls_analysis",help="Output file prefix (default: xls_analysis)")
    parser.add_argument("-l","--log-file",type=str,default="xls_analysis.log",help="Log file (default: xls_analysis.log)")
    parser.add_argument("--log-to-file",action="store_true",default=False,help="Enable logging to file (default: off)")
    args=parser.parse_args()
    global LOG_TO_FILE
    LOG_TO_FILE=args.log_to_file
    setup_logging(args.log_file)
    tracemalloc.start()
    await analyze_xls(args.filename,args.output_prefix)
if __name__=="__main__":asyncio.run(main())
