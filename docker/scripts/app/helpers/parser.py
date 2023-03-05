import os

def parse_file_names(enade_folder):
    fls_result = ""
    for directory, sub_folders, files in os.walk(enade_folder):
        if directory == "./enade2017/microdados_Enade_2017_LGPD/2.DADOS":
            fls_result = files
    return fls_result