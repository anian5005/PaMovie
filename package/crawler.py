import re
from package.nlp import preprocess

def fuzzy_name(str):
    result_list = []
    original = str
    # print('original_name', original)

    # replace space
    full_str = original.replace('  ', ' ').strip().strip('!').strip('！')
    clean_str = preprocess(full_str)
    result_list.append(full_str)

    # split comma
    comma = r':|：'
    if re.search(comma, full_str):
        # print('check', re.search(comma, full_str))
        split_str = re.split(comma, full_str)
        for str in split_str:
            str = str.lstrip(' ').rstrip(' ')
            result_list.append(str)

    # print(result_list)
    return result_list

