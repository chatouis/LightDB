
def merge_sql_file():
    import os
    import re
    os.chdir("/home/yzj/workspace/LightDB/scripts")
    f = lambda x : int(x[5:][:-4])
    files = sorted(os.listdir("/home/yzj/workspace/LightDB/samples/input/"), key=f)
    sqls = []
    for file in files:
        if file.endswith('.sql'):
            with open("/home/yzj/workspace/LightDB/samples/input/" + file, 'r') as f:
                sqls.append(f.read())
    with open('merged.sql', 'w') as f:
        f.write(''.join(sqls))

if __name__ == '__main__':
    merge_sql_file()