import os
import sys
from datetime import datetime, timezone, timedelta


def timedelta_second_converter(my_datetime):
    # Convert timedelta to total seconds
    return my_datetime.total_seconds()


def get_current_taiwan_datetime():
    current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    convert_time = current_time.astimezone(timezone(timedelta(hours=8)))
    return convert_time


def get_current_taiwan_time_str():
    current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    convert_time = current_time.astimezone(timezone(timedelta(hours=8)))
    taiwan_time = convert_time.strftime('%Y-%m-%d %H:%M:%S')
    return taiwan_time


def taiwan_time_converter(my_datetime):
    t1 = my_datetime.replace(tzinfo=timezone.utc)
    t2 = t1.astimezone(timezone(timedelta(hours=8)))
    convert_time = t2.strftime("%Y-%m-%d %H:%M:%S")
    return convert_time


def log_time():
    ymdhm = datetime.now().strftime('%Y-%m-%d %H:%M')
    ymd = datetime.now().strftime('%Y-%m-%d')
    return str(ymdhm)


def get_log(process, start, status, imdb_id, word=None, msg=None, result_count=None):
    # convert str to int
    if result_count is not None:
        int_result_count = int(result_count)
    else:
        int_result_count = None

    current_time = str(get_current_taiwan_datetime().strftime('%Y-%m-%d %H:%M:%S'))
    end = get_current_taiwan_datetime()
    spent = str(round(timedelta_second_converter(end - start), 2))

    log = {
        'func_name': process,
        'log_time': current_time,
        'imdb_id': imdb_id,
        'word': word,
        'spent': spent,
        'status_code': status,
        'result_count': int_result_count,
        'msg': msg
    }
    return log


def exception_info():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    # error_info = 'FILE: ' + fname + ', TYPE: ' + str(exc_type) + ', LINE: ' + str(exc_tb.tb_lineno) + ', MSG: "' + str(more_info)+'"'
    error_info = {'file_name': fname, 'exc_type': str(exc_type), 'line': str(exc_tb.tb_lineno)}
    return error_info


# temp
def retry(func, times):
    count = 0
    while count < times:
        try:
            count = count + 1
            result = func()
            break
        except Exception as er:
            print(er)
            # if try many times and fail, return error message
            if count == times - 1:
                return {'status': 2, 'msg': str(er)}
            else:
                pass


def add_item_into_dict(my_dict, my_key, item, struct=None):
    if my_dict.get(my_key, None) is None:
        if struct == 'list':
            my_dict[my_key] = [item]
        elif struct == 'dict':
            my_dict[my_key] = {}
            # item type is dict
            my_dict[my_key].update(item)
        elif struct is None:
            my_dict.update({my_key: item})
        else:
            print('the struct was wrong')
    else:
        if struct == 'list':
            my_dict[my_key].append(item)
        elif struct == 'dict':
            my_dict[my_key].update(item)
        elif struct is None:
            my_dict.update({my_key: item})
