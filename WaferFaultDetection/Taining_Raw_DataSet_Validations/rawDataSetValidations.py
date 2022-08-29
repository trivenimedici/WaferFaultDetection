from datetime import datetime
from os import listdir
import shutil
from pathlib import Path
import shutil
from App_Logger.logger import app_logger
import json
import os
import re
import pandas as pd
class raw_Data_Validations:
    """
    this class is for handling all the validations on Raw Training data set

    """
    def __init__(self,path):
        self.app_log = app_logger
        self.batchfiles_dir = path
        self.dataschema_path="schema_training.json"

    def getValuesfromSchema(self):
        """
            Method Name: getValuesfromSchema
            Description: This function is to get the values from schema traning json file
            Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number_of_Columns
            On Failure: Raise ValueError,KeyError,Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        try:
            with open(self.dataschema_path,'r') as f:
                dic=json.load(f)
                f.close()
            filename =dic['SampleFileName']
            LengthOfDateStampInFile=dic['LengthofDateStampinFileName']
            LengthOfTimeStampInFile=dic['LengthofTimeStampinFileName']
            Number_of_Columns=dic['TotalNoOfColumnsinFile']
            column_names=dic['ColNameAndType']
            file=open('Training_Logs/valuesfromSchemaValidationLog.txt','+a')
            message="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % Number_of_Columns + "\n"
            self.app_log.log(file,message)
            file.close()
        except ValueError:
            file=open('Training_Logs/valuesfromSchemaValidationLog.txt','+a')
            self.app_log.log(file,'ValueError:Value not found in the schema_training.json')
            file.close()
            raise ValueError
        except KeyError:
            file=open('Training_Logs/valuesfromSchemaValidationLog.txt','+a')
            self.app_log.log(file,'KeyError:Key not found in the schema_training.json')
            file.close()
            raise KeyError
        except Exception as e:
            file=open('Training_Logs/valuesfromSchemaValidationLog.txt','+a')
            self.app_log.log(file,str(e))
            file.close()
            raise e
        return filename,LengthOfDateStampInFile,LengthOfTimeStampInFile,Number_of_Columns,column_names

    def FileNameRegexCreation(self):
        """
            Method Name: FileNameRegexCreation
            Description: This function is to create regular expression for file name validation
            Output: regexToValidate
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        regexToValidate="['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regexToValidate
    
    def createDirectoryForGoodBadRawData(self):
        """
            Method Name: createDirectoryForGoodBadRawData
            Description: This function is to create directory for storing good and bad raw data
            Output: OError
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        try:
            path=os.path.join('Training_Raw_files_validated/','Good_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
            path=os.path.join('Training_Raw_files_validated/','Bad_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.app_log.log(file,"Error while creating Directory %s:" % e)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):
        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This function is to delete existing good date training folder directory
            Output: OError
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        try:
            path='Training_Raw_files_validated/'
            if os.path.isdir(path+'Good_Raw/'):
                shutil.rmtree(path+'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.app_log.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.app_log.log(file,"Error while deleting Good Raw Data Directory %s:" % e)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):
        """
            Method Name: deleteExistingBadDataTrainingFolder
            Description: This function is to delete existing bad date training folder directory
            Output: OError
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        try:
            path='Training_Raw_files_validated/'
            if os.path.isdir(path+'Bad_Raw/'):
                shutil.rmtree(path+'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.app_log.log(file,"BadRaw directory deleted successfully!!!")
                file.close()
        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.app_log.log(file,"Error while deleting BaD Raw Data Directory %s:" % e)
            file.close()
            raise OSError
    
    def moveBadDataFilesToArchiveBad(self):
        """
            Method Name: moveBadDataFilesToArchiveBad
            Description: This method deletes the directory made  to store the Bad Data
                after moving the data in an archive folder. We archive the bad
                files to send them back to the client for invalid data issue.
            Output: OError
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        now=datetime.now()
        current_date=now.date()
        current_time=now.strftime('%H%M%S')
        try:
            source='Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path='TrainingArchiveBadData'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest='TrainingArchiveBadData/BadData_'+str(current_date)+'_'+str(current_time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files=os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source+f,dest)
                file=open('Training_Logs/GeneralLog.txt','a+')
                self.app_log.log(file,'Bad files moved to archieve')
                path='Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.app_log.log(file,'Bad data files are removed from Bad Raw Data folder')
                file.close()
        except Exception as e:
            file = open('Training_Logs/GeneralLog.txt','a+')
            self.app_log.log('there is error while moving bad raw data files to archieve:: %s' % e)
            file.close()
            raise e
                
    def validateBatchFileName(self,regexToValidate,lengthofdatestampinfile,lengthoftimestampinfile):
        """
            Method Name: validateBatchFileName
            Description: This function validates the name of the training csv files as per given name in the schema!
                Regex pattern is used to do the validation.If name format do not match the file is moved
                to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        onlyfiles=[f for f in listdir(self.batchfiles_dir)]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if(re.match(regexToValidate,filename)):
                    splitAtDot=re.split('.csv',filename)
                    splitAtDot=re.split('_',splitAtDot[0])
                    if len(splitAtDot[1])==lengthofdatestampinfile:
                        if len(splitAtDot[2] == lengthoftimestampinfile):
                            shutil.copy('Training_Batch_Files/'+filename,'Training_Raw_files_validated/Good_Raw')
                            self.app_log.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
                        else:
                            shutil.copy('Training_Batch_Files/'+filename,'Training_Raw_files_validated/Bad_Raw')
                            self.app_log.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy('Training_Batch_Files/'+filename,'Training_Raw_files_validated/Bad_Raw')
                        self.app_log.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy('Training_Batch_Files/'+filename,'Training_Raw_files_validated/Bad_Raw')
                    self.app_log.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
            f.close()
        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.app_log.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validateColumnLength(self,nuberofcolumns):
        """
            Method Name: validateColumnLength
            Description: This function validates the name of the training csv files as per given name in the schema!
                Regex pattern is used to do the validation.If name format do not match the file is moved
                to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception,OSError
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        try:
            f=open("Training_Logs/columnValidationLog.txt", 'a+')
            self.app_log.log(f,'column length validation started')
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv=pd.read_csv('Training_Raw_files_validated/Good_Raw/'+file)
                if csv.shape[1] == nuberofcolumns:
                    self.app_log.log(f,'file is have correct columns count')
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.app_log.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                self.app_log.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.app_log.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.app_log.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
            Method Name: validateMissingValuesInWholeColumn
            Description: This function validates  if any column in the csv file has all values missing.
                             If all the values are missing, the file is not suitable for processing.
                            such files are moved to bad raw data.
            Output: None
            On Failure: Exception,OSError
            Written By: triveni
            Version: 1.0
            Revisions: None
                """
        try:
            f=open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.app_log.log(f,"Missing Values Validation Started!!")
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv=pd.read_csv('Training_Raw_files_validated/Good_Raw/'+file)
                count=0
                for columns in csv:
                    if(len(csv[columns]) - csv[columns].count())==len(csv[columns]):
                        count+=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.app_log.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.app_log.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.app_log.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()















        
