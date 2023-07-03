import os, datetime, sys
import logging as log
import pandas as pd
import regex as re

input_test = "C:\Users\luiorio\OneDrive - Capgemini\Desktop"
def get_dir_files(user_input=input_test):
    print("Inserire percorso.")
    user_input = input()
    if user_input == "":
        lista_file = os.listdir(os.getcwd())
        print(f"Usato il path: {os.getcwd()}")
    else:
         lista_file = os.listdir(os.chdir(user_input))
         print(f"Usato il path: {user_input}")

    
    lista_excels = list()
    for files in lista_file:
        if files.endswith(('.csv','.xls')):
                lista_excels.append(files)
        
    if len(lista_excels) != 0:
        print("Trovati file in formato excel.")

    return lista_excels




# input_file =get_dir_files()[-1] #"Liquid_rec_ppvv.xls"

def reading_dataset(input_file):
    if input_file.endswith('.xls'):
        try:
            df = pd.read_excel(input_file)
            df.fillna("null", inplace=True)
            log.info(df.head())
            return df
        except:
            log.error("Errore nella funzione reading_dataset()")

    elif  input_file.endswith('.csv'):
        try:
            df = pd.read_csv(input_file)
            df.fillna("null", inplace=True)
            log.info(df.head())
            return df
        except:
            log.error("Errore nella funzione reading_dataset()")


            
# df = reading_dataset(input_file)



def get_params(input_file):
        FILENAME = "insert_" + input_file.split(".")[0].upper()+"_"+ str(datetime.datetime.now().strftime('%Y-%m-%d')) +".sql"
        # CREAZIONE DEL FILE DI INSERT
        FILEPATH = os.path.join(os.getcwd(),FILENAME)
        log.info(f"Creato il file: {FILENAME}")
        return FILEPATH

def create_insert(limit_rows=0, db="db", table="table"):
        input_file = input()
        FILEPATH = get_params(input_file)
        df = reading_dataset(FILEPATH)
        with open(FILEPATH, "w") as insert:
                # SCRITTURA DELLA PRIMA RIGA
                insert.write(f"USE {db}.{table};\n")
                insert.close()   
        col_number = len(df.columns)
        if limit_rows == 0:
                range_rows = range(0,len(df))
        elif limit_rows != 0 and limit_rows <= len(df): 
                range_rows = range(0,limit_rows)
        elif limit_rows > len(df):
                log.warning(f"The parameter range rows ({range_rows}) exceed the maximum number of rows of the csv ({len(df)}).")
        # SCRITTURA DELLE INSERT (FULL MODE)
        log.info("Inizio scrittura delle insert....")
        for row in range_rows:
                row_values = list()
                for i in range(0, col_number):
                        row_values.append(df.loc[row][i])
                        query = f'''INSERT INTO {db}.{table}VALUES ({row_values});\n'''
                with open(FILEPATH, "a") as insert:
                        insert.write(query)
                        insert.close()
        # TERMINE DI SCRITTURA
        log.info(f'''Query eseguita con successo. Il file è pronto al path:\n
              {FILEPATH}.''')


def null_values_handling():
        FILEPATH = get_params(input_file)
        new_file_content = ""
        with open (FILEPATH,"r") as file:
                for line in file:
                        stripped_line = line.strip()
                        new_line = stripped_line.replace("'null'","null")
                        new_file_content += new_line+"\n"
                file.close()
        with open (FILEPATH,"w") as new_file:
                new_file.write(new_file_content)
                new_file.close()


def bool_values_handling():
        FILEPATH = get_params(input_file)
        new_line = ''
        new_file_content = ""
        true = "true"
        false = "false"
        with open (FILEPATH,"r") as file:
                for line in file:
                        stripped_line = line.strip()
                        new_line = re.sub(pattern=r'(?i)\b' + re.escape(true) + r'\b',repl="True",string = stripped_line)
                        new_line = re.sub(pattern=r'(?i)\b' + re.escape(false) + r'\b',repl="False",string = new_line)
                        new_file_content += new_line+"\n"                       
                        new_file_content = re.sub("'True'",repl=str(True),string = new_file_content)
                        new_file_content = re.sub("'False'",repl="False",string = new_file_content)
                        # new_file_content = re.sub("\"True\"",repl=str(True),string = new_file_content)
                        # new_file_content = re.sub("\"False\"",repl="False",string = new_file_content)
                file.close()
        with open (FILEPATH,"w") as new_file:
                new_file.write(new_file_content)
                new_file.close()


def quotes_values_handling():
        FILEPATH = get_params(input_file)
        new_file_content = ""
        with open (FILEPATH,"r") as file:
                for line in file:
                        stripped_line = line.strip()
                        new_line = line.replace("\"","")
                        new_file_content += new_line+"\n"
                file.close()
        with open (FILEPATH,"w") as new_file:
                        new_file.write(new_file_content)
                        new_file.close()



if __name__ == "__main__":
    get_dir_files()
    input_file_name = input()
    create_insert(input_file=input_file_name)
