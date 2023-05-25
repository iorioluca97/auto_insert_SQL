import os, datetime, sys
import logging as log
import pandas as pd
import regex as re
import time


class insertExcelSQL:
    def __init__(self, filepath, schema):
        '''Define the full path (absolute) of the file to create the sql inserts.'''
        self.SQL_SCHEMA = schema
        self.filepath = filepath
        self.fileName = self.filepath.split("\\")[-1]
        self.abspath = self.filepath.split(self.fileName)[0]


    def ifExists(self):
        '''Check the existence of the path'''
        try:
            os.chdir(self.abspath)
            if  self.fileName in os.listdir(self.abspath):
                print(f"File trovato al path: {self.abspath}")
                return True
            else:
                print("File non presente nel path indicato")
        except:
            print("Path non valido")


    def defineFileOutputName(self):
        '''Define the naming convention based on the file name and date time.'''
        OUTPUT_FILENAME = "insert_" + self.fileName.split(".")[0].upper()+"_"+ str(datetime.datetime.now().strftime('%Y-%m-%d')) +".sql"
        
        # CREAZIONE DEL FILE DI INSERT
        WD = "insertExcelSQL"
        if not os.path.exists(os.path.join(self.abspath,WD)):
            os.chdir(self.abspath)
            os.mkdir(WD)
        OUTPUT_FILEPATH = os.path.join(self.abspath,WD,OUTPUT_FILENAME)
        print(f"Creato il file al path: {OUTPUT_FILEPATH}")
        
        self.OUTPUT_FILEPATH = OUTPUT_FILEPATH
        return self.OUTPUT_FILEPATH
         
    def create_SQL_file(self):
        '''Define the schema to be used in each sql statement to perform
        the query. It can be inserted when launching main application.'''
       
        # if "." in self.SQL_SCHEMA:
        #       db = self.SQL_SCHEMA.split(".")[0]
        #       table = self.SQL_SCHEMA.split(".")[1]

        with open(self.OUTPUT_FILEPATH, "w") as insert:
                # SCRITTURA DELLA PRIMA RIGA
                insert.write(f"USE {self.SQL_SCHEMA};\n")
                insert.close()   

        '''You can set a limit amount of insert by modifyng the limit_rows param, otherwise the sql file will be comprehensive by all insert rows.'''
        # self.limit_rows = int(input())
        self.limit_rows=200
        self.read_df()
        col_number = len(self.df.columns)

        
        if self.limit_rows == 0:
                range_rows = range(0,len(self.df))
        elif self.limit_rows != 0 and self.limit_rows <= len(self.df): 
                range_rows = range(0,self.limit_rows)
        elif self.limit_rows > len(self.df):
                print(f"The parameter range rows ({range_rows}) exceed the maximum number of rows of the csv ({len(self.df)}).")

        # SCRITTURA DELLE INSERT (FULL MODE)
        print("Inizio scrittura delle insert....")
        for row in range_rows:
                row_values = list()
                for i in range(0, col_number):
                        row_values.append(self.df.loc[row][i])
                        query = f'''INSERT INTO {self.SQL_SCHEMA} VALUES ({row_values});\n'''
                with open(self.OUTPUT_FILEPATH, "a") as insert:
                        insert.write(query)
                        insert.close()


        self.null_values_handling()
        self.bool_values_handling()
        self.parenthesis_handling()


        # TERMINE DI SCRITTURA
        print(f'''Query eseguita con successo. Il file è pronto al path:\n{self.OUTPUT_FILEPATH}.''')


    def null_values_handling(self):
        print("Inizio gestione valori nulli.")

        new_file_content = ""

        with open (self.OUTPUT_FILEPATH,"r") as file:
                for line in file:
                        stripped_line = line.strip()
                        new_line = stripped_line.replace("'null'","null")
                        new_file_content += new_line+"\n"
                file.close()

        with open (self.OUTPUT_FILEPATH,"w") as new_file:
                new_file.write(new_file_content)
                new_file.close()
        print("Termine gestione valori nulli.")

    def parenthesis_handling(self):
        print("Inizio gestione parentesi.")

        new_file_content = ""

        with open (self.OUTPUT_FILEPATH,"r") as file:
                for line in file:
                        stripped_line = line.strip()
                        new_line = stripped_line.replace("[","")
                        new_line = new_line.replace("]","")
                        new_file_content += new_line+"\n"
                file.close()

        with open (self.OUTPUT_FILEPATH,"w") as new_file:
                new_file.write(new_file_content)
                new_file.close()
        print("Termine gestione parentesi.")


    def bool_values_handling(self):
        print("Inizio gestione valori bool.")

        new_line = ''
        new_file_content = ""
        true = "true"
        false = "false"


        with open (self.OUTPUT_FILEPATH,"r") as file:
                
                for line in file:
                        stripped_line = line.strip()
                        new_line = re.sub(pattern=r'(?i)\b' + re.escape(true) + r'\b',repl="True",string = stripped_line)
                        new_line = re.sub(pattern=r'(?i)\b' + re.escape(false) + r'\b',repl="False",string = new_line)
                        new_file_content += new_line+"\n"
                        
                        new_file_content = re.sub("'True'",repl=str(True),string = new_file_content)
                        new_file_content = re.sub("'False'",repl=str(False),string = new_file_content)
               
                        new_file_content = re.sub("\"True\"",repl=str(True),string = new_file_content)
                        new_file_content = re.sub("\"False\"",repl=str(False),string = new_file_content)
                        

                file.close()
        with open (self.OUTPUT_FILEPATH,"w") as new_file:
                new_file.write(new_file_content)
                new_file.close()
        print("Termine gestione valori bool.")



    def read_df(self):
        if self.fileName.endswith('.xls'):
                df = pd.read_excel(self.filepath)
                df.fillna("null", inplace=True)
                
        elif self.fileName.endswith('.csv'):
                df = pd.read_csv(self.filepath)
                df.fillna("null", inplace=True)
        else: print(f"Metodo di lettura del df ancora non implementato: {self.fileName.endswith('.csv')}")
        
        self.df = df
        return df
    
       


   
    def runSQL(self):
       self.ifExists()
       self.defineFileOutputName()
       self.read_df()
       self.create_SQL_file()





def main():
      filepath = sys.argv[1]
      schema = sys.argv[2]
      # filepath = 'C:\\Users\\luiorio\\OneDrive - Capgemini\\Desktop\\export.csv'
      
      x = insertExcelSQL(filepath=filepath, schema = schema)
      t = time.process_time()
      x.runSQL()
      elapsed_time = time.process_time() - t
      print(f"Elapsed time: {elapsed_time}")

      
main()
