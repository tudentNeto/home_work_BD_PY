import psycopg2
import configparser


config = configparser.ConfigParser()
config.read('config.ini')
DATABASE = config.get('Settings', 'bd_name')
USER = config.get('Settings', 'user_name')
PASSWORD = config.get('Settings', 'password')

LIST_FIELDS_C_V=['id','Имя', 'Фамилия', 'email']

LIST_FIELDS_P_V=['id','Номер телефона', 'Комментарий', 'Код клиента']


def create_tables(cur_cursor,s_sql):
    cur_cursor.execute(s_sql)


def get_list_fields(cur_cursor, table_name):
    cur_cursor.execute('''SELECT column_name, is_nullable, ordinal_position,data_type 
        FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name =%s; 
        ''', (table_name,))
    list_fields_r = cur.fetchall()
    list_fields = []
    tup_field = []
    for i in range(len(list_fields_r)):
        for el_list in list_fields_r:
            if el_list[2] == i + 1:
                for el_tup in el_list:
                    tup_field.append(el_tup)
        list_fields.append(tuple(tup_field))
        tup_field = []
    return list_fields


def add_row(cur_cursor, table_name, find_id_client=False):
    if table_name == 'client':
        list_values = list_c
        list_name = LIST_FIELDS_C_V
    elif table_name =='phone':
        list_values=list_p
        list_name = LIST_FIELDS_P_V
    else:
        list_values=[]
    if list_values:
        s_field_name='('
        for i in range(1,len(list_values)):
            s_field_name=s_field_name + list_values[i][0] + ', '
        s_field_name=s_field_name[:-2] + ')'
        print('заполняем таблицу : ' + table_name)
        s_values='('
        for i in range(1,len(list_name)):
            while True:
                s_val = input('Введите значение для поля <' + list_name[i] + '>: ')
                if list_values[i][2] == 'NO':
                    if not(s_val):
                        continue
                lit=("" if list_values[i][3] in ('integer') else "'")
                s_values=s_values + lit + s_val + lit + ", "
                break
        s_values=s_values[:-2] + ')'
        s_sql = 'INSERT INTO ' + table_name + s_field_name + ' VALUES ' +  s_values + ' RETURNING id;'
        cur_cursor.execute(s_sql)
        return cur_cursor.fetchone()[0]
    else:
        return 0

def get_info_client(cur_cursor, list_criteria):
    lit='x'
    s_sql='SELECT * FROM client'
    s_where=' WHERE '
    for i in range(1, len(list_c)):
        if list_criteria[i-1]!=lit:
            spec_symb="" if list_c[i][3] in ('integer') else "'"
            s_where=s_where + list_c[i][0] + '=' + spec_symb + list_criteria[i-1] + spec_symb + ' AND '
    s_where=s_where[:-5] + ';'
    cur_cursor.execute(s_sql + s_where)
    return cur_cursor.fetchone()


def get_id_client(cur_cursor, list_criteria):
    return get_info_client(cur_cursor, list_criteria)[0]


def upd_client_info(cur_cursor, list_new_values, id):
    lit='x'
    s_sql='UPDATE client SET '
    s_where=" WHERE id=" + str(id) + ';'
    s_set=''
    for i in range(1, len(list_c)):
        if list_new_values[i-1]!=lit:
            spec_symb="" if list_c[i][3] in ('integer') else "'"
            s_set=s_set + list_c[i][0] + '=' + spec_symb + list_new_values[i-1] + spec_symb + ', '
    s_set=s_set[:-2]
    cur_cursor.execute(s_sql + s_set + s_where)


def del_phone(cur_cursor, id):
    s_sql='DELETE FROM phone WHERE id_client=' + str(id)
    cur_cursor.execute(s_sql)


def del_client(cur_cursor, id):
    s_sql='DELETE FROM client WHERE id=' + str(id)
    cur_cursor.execute(s_sql)


with (psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD) as conn):
    with conn.cursor() as cur:
        cur.execute('''DROP TABLE phone;
                        DROP TABLE client;
                    ''')
        # 1 создание таблиц
        create_sql_c='''CREATE TABLE client (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(60) NOT NULL,
                    last_name VARCHAR(60) NOT NULL,
                    email VARCHAR(80) NOT NULL
                    );'''
        create_tables(cur,create_sql_c)
        conn.commit
        create_sql_p='''CREATE TABLE phone (
                    id SERIAL PRIMARY KEY,
                    phone_num VARCHAR(60) NOT NULL,
                    comment text,
                    id_client INTEGER NOT NULL,
                    FOREIGN KEY (id_client) REFERENCES client(id) ON DELETE CASCADE
                    );'''
        create_tables(cur, create_sql_p)
        conn.commit
        list_c=get_list_fields(cur, 'client')
        list_p = get_list_fields(cur, 'phone')
        # 2-3 массовый ввод клиентов + на каждого клиента телефоны
        count_client=int(input('Сколько клиентов необходимо ввести?'))
        n_row = count_client
        while n_row != 0:
            id_client=add_row(cur,'client')
            if id_client!=0:
                count_phone = int(input('Сколько номеров телефонов будет внесено у клиента?'))
                if count_phone:
                    k_row = count_phone
                    print('ВНИМАНИЕ! в поле <Код клиента> укажите ' + str(id_client) + " !")
                    while k_row!=0:
                        id_phone = add_row(cur, 'phone')
                        if id_phone == 0:
                            print('Ошибка!')
                            count_phone = 0
                            break
                        k_row -= 1
                print('Инфо о клиенте внесена (сохранено ' + str(count_phone) + ' телефонов)')
                n_row -= 1
            else:
                print('Ошибка!')
                break
        # 2 одиночный ввод клиента
        print('Введите информацию о клиенте')
        add_row(cur, 'client')
        # получить id-client для одиночного ввода телефона
        print('Ввод номера телефона клиента')
        print('Что Вам известно о клиенте? (если параметр не определен ставим <x>)')
        list_criteria_info=[]
        s_el_criteria=''
        for i in range(1, len(LIST_FIELDS_C_V)):
            while True:
                s_el_criteria = input('Введите значение для поля <' + LIST_FIELDS_C_V[i] + '>: ')
                if not (s_el_criteria):
                   continue
                else:
                   break
            list_criteria_info.append(s_el_criteria)
            s_el_criteria=''
        id_client_for_add_phone=get_id_client(cur,list_criteria_info)
        print('ВНИМАНИЕ! код клиента, опеределенный по вашему запросу = ', id_client_for_add_phone)
        print('Введите в поле <Код клиента> ', id_client_for_add_phone)
        # 3  одиночный ввод телефона по найденному id клиента
        add_row(cur, 'phone')

        # получить id-client для изменения информации
        print('Что Вам известно о клиенте, информацию о котором надо изменить? (если параметр не определен ставим <x>)')
        list_criteria_upd = []
        s_el_criteria_upd = ''
        for i in range(1, len(LIST_FIELDS_C_V)):
            while True:
                s_el_criteria_upd = input('Введите значение для поля <' + LIST_FIELDS_C_V[i] + '>: ')
                if not (s_el_criteria_upd):
                    continue
                else:
                    break
            list_criteria_upd.append(s_el_criteria_upd)
            s_el_criteria_upd = ''
        id_client_for_upd = get_id_client(cur, list_criteria_upd)
        if id_client_for_upd:
            print('Введите новые значения полей (если поле не нужно менять ставим <x>)')
            list_criteria_new = []
            s_el_criteria_new = ''
            for i in range(1, len(list_c)):
                while True:
                    s_el_criteria_new = input('Введите новое значение для поля <' + LIST_FIELDS_C_V[i] + '>: ')
                    if not (s_el_criteria_new):
                        continue
                    else:
                        break
                list_criteria_new.append(s_el_criteria_new)
                s_el_criteria_new = ''
            # 4 изменение информации о клиенте
            upd_client_info(cur, list_criteria_new, id_client_for_upd)

        # 5 удалить телефон
        # получить id_client для удаления телефона
        print('Что Вам известно о клиенте, номер телефона которого надо удалить? (если параметр не определен ставим <x>)')
        list_criteria_del_phone = []
        s_el_criteria_del_phone = ''
        for i in range(1, len(LIST_FIELDS_C_V)):
            while True:
                s_el_criteria_del_phone = input('Введите значение для поля <' + LIST_FIELDS_C_V[i] + '>: ')
                if not (s_el_criteria_del_phone):
                    continue
                else:
                    break
            list_criteria_del_phone.append(s_el_criteria_del_phone)
            s_el_criteria_del_phone = ''
        id_client_for_del_phone = get_id_client(cur, list_criteria_del_phone)
        del_phone(cur, id_client_for_del_phone)
        conn.commit
        # 6 удалить клиента по критериям
        # получить id_client для удаления
        print('Что Вам известно о клиенте, которого надо удалить? (если параметр не определен ставим <x>)')
        list_criteria_del_client = []
        s_el_criteria_del_client = ''
        for i in range(1, len(LIST_FIELDS_C_V)):
            while True:
                s_el_criteria_del_client = input('Введите значение для поля <' + LIST_FIELDS_C_V[i] + '>: ')
                if not (s_el_criteria_del_client):
                    continue
                else:
                    break
            list_criteria_del_client.append(s_el_criteria_del_client)
            s_el_criteria_del_client = ''
        id_client_for_del_client = get_id_client(cur, list_criteria_del_client)
        del_client(cur, id_client_for_del_client)
        conn.commit





