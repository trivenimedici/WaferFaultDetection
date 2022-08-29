from datetime import datetime


class app_logger:
    def __init__(self) -> None:
        pass

    def log(self,file_object,log_message):
        self.now = datetime.now()
        self.current_date = self.now.date()
        self.current_time = self.now.strftime('%H:%M:%S')
        file_object.write(str(self.current_date)+'/'+str(self.current_time)+ "\t\t" + log_message +"\n")