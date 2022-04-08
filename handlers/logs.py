import os


def logs(*args):
    msg = ",".join([str(arg) for arg in args])
    file_name = '\\logs.txt'
    path = os.getcwd() + '\\logs_data' + file_name

    with open(path, 'w+') as logs_file:
        first_line = logs_file.readline()
        if len(first_line) > 0:
            logs_file.writelines('\n'.join([' ', msg]))
        else:
            logs_file.write('date, err_val, msg, critical')
            logs_file.write('\n' + msg)
    return msg
