import os

list_of_db = ['xomdata','xomtodo','xomdone','xomsubmitted','xomtocheck']

exec_period = 10

query_period = 100 # days period over which we should search in database (when xom will be running smoothly can beset to 1d)

xomfolder= os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../")
output_folder = xomfolder + "/output/"
job_folder = output_folder + "/job_files/"
result_folder = output_folder + "/result/"
analysis_code_folder = xomfolder + "/analyses/"
tool_config_folder = xomfolder + "/config/"



