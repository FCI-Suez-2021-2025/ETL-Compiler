from app.core.errors import ParserError
import ply.yacc

start = "start"


def p_start(p):
    """start : select
    | insert
    | update
    | delete"""
    p[0] = p[1]


def p_empty(p):
    "empty :"
    pass


def p_error(p):
    value, line_number, position = p.value, p.lineno, p.lexpos
    raise ParserError(
        f"Syntax error at token '{value}' on line {line_number}, position {position}.",
        p.value,
        p.lineno,
        p.lexpos,
    )


# else:
#         raise ParserError("Syntax error at EOF")


###########################
# ==== SELECT STATEMENT​​ ====
###########################


def p_select(p):
    """select : SELECT distinct select_columns into_statement FROM DATASOURCE where group order limit_or_tail SIMICOLON"""
    if type(p[3]) == str:
        p[3] = "'" + p[3] + "'"

    file_type, file_path = p[6].split(":", 1)
    if p[4]:
        load_type, load_path = p[4].split(":", 1)
    p[0] = (
        f"from app import etl\n"
        f"\n"
        f"extracted_data = etl.extract('{file_type}','{file_path}')\n"
        f"transformed_data = etl.transform_select(\n"
        f"   extracted_data,\n"
        f"   {{\n"
        f"        'COLUMNS':  {p[3]},\n"
        f"        'DISTINCT': {p[2]},\n"
        f"        'FILTER':   {p[7]},\n"
        f"        'GROUP':    {p[8]},\n" # new change
        f"        'ORDER':    {p[9]},\n" 
        f"        'LIMIT_OR_TAIL':    {p[10]},\n"
        f"    }}\n"
        f")\n"
        f""
        f"{f"etl.load(transformed_data,'{load_type}','{load_path}')" if p[4] else "" }\n"
    )


###########################
# ==== INSERT STATEMENT ====
###########################


def p_insert(p):
    "insert : INSERT INTO DATASOURCE icolumn VALUES insert_values SIMICOLON"

    p[3] = str(p[3]).replace("\\", "\\\\")
    p[0] = (
        f"from app import etl\n"
        f"import pandas as pd\n"
        f"\n"
        f"values = {p[6]}\n"
        f"data_destination = '{p[3]}'\n"
        f"data = pd.DataFrame(values, columns={p[4]})\n"
        f"etl.load(data, data_destination)\n"
    )


###########################
# ==== Update STATEMENT ====
###########################
def p_update(p):
    "update : UPDATE DATASOURCE SET assigns where SIMICOLON"
    p[0] = None


###########################
# ==== DELETE STATEMENT​​ ====
###########################


def p_delete(p):
    "delete : DELETE FROM DATASOURCE where"
    p[0] = None


##########################
# ====== COMPARISON =======
##########################


def p_logical(p):
    """logical :  EQUAL
    | NOTEQUAL
    | BIGGER_EQUAL
    | BIGGER
    | SMALLER_EQUAL
    | SMALLER"""
    p[0] = p[1]


##########################
# ====== WHERE CLAUSE =====
##########################


def p_where(p):
    "where : WHERE conditions"
    p[0] = p[2]


def p_where_empty(p):
    "where : empty"
    p[0] = None


def p_cond_parens(p):
    "conditions : LPAREN conditions RPAREN"
    p[0] = p[2]


def p_cond_3(p):
    """conditions : conditions AND conditions
    | conditions OR conditions
    | exp LIKE STRING
    | exp logical exp"""
    p[0] = {"type": p[2], "left": p[1], "right": p[3]}


def p_conditions_not(p):
    "conditions : NOT conditions"
    p[0] = {"type": p[1], "operand": p[2]}


##########################
# ========== EXP ==========
##########################


def p_exp(p):
    """exp : column
    | STRING
    | NUMBER"""

    p[0] = p[1]


##########################
# ========== EXP ==========
##########################
def p_NUMBER(p):
    """NUMBER : NEGATIVE_INTNUMBER
    | POSITIVE_INTNUMBER
    | FLOATNUMBER"""
    p[0] = p[1]


###########################
# ======== Distinct ========
###########################


def p_distinct(p):
    """distinct : DISTINCT"""
    p[0] = True


def p_distinct_empty(p):
    """distinct : empty"""
    p[0] = False


###########################
# ======== COLUMNS =========
###########################
def p_column(p):
    """column : COLNUMBER
    | BRACKETED_COLNAME
    | SIMPLE_COLNAME"""
    p[0] = p[1]


# def p_column_name(p):
#     """column_name : BRACKETED_COLNAME
#     | SIMPLE_COLNAME"""
#     p[0] = p[1]


def p_columns(p):
    """columns : columns COMMA columns"""
    p[0] = []
    p[0].extend(p[1])
    p[0].extend(p[3])


def p_columns_base(p):
    """columns : column"""
    p[0] = [p[1]]


###########################
# ===== SELECT COLUMNS​​ =====
###########################


def p_select_columns_all(p):
    "select_columns : TIMES"
    p[0] = "__all__"


def p_select_columns(p):
    "select_columns : columns"
    p[0] = p[1]


###########################
# ========= Into ===========
###########################


def p_into_statement(p):
    "into_statement : INTO DATASOURCE"
    p[0] = p[2]


def p_into_statement_empty(p):
    "into_statement : empty"


###########################
# ======= Order by =========
###########################


def p_order(p):
    """order : ORDER BY column way"""
    p[0] = (p[3], p[4])


def p_order_empty(p):
    "order : empty"
    p[0] = None


def p_way_asc(p):
    """way : ASC
    | empty"""
    p[0] = "asc"


def p_way_desc(p):
    "way : DESC"
    p[0] = "desc"


###########################
# ======= Group by =========
###########################


def p_group(p):
    """group : GROUP BY columns"""
    p[0] = p[3]  # The list of columns to group by

def p_group_empty(p):
    "group : empty"
    p[0] = None

# def p_groub_columns(p):
#     """columns : column
#                | columns COMMA column"""
#     if len(p) == 2:  # Single column
#         p[0] = [p[1]]
#     else:  # Multiple columns
#         p[0] = p[1] + [p[3]]


###########################
# ========= Limit & Tail ==========
###########################


def p_limit_or_tail(p):
    """limit_or_tail : LIMIT POSITIVE_INTNUMBER
    | TAIL POSITIVE_INTNUMBER"""
    p[0] = (p[1], p[2])


def p_limit_or_tail_empty(p):
    """limit_or_tail : empty"""
    p[0] = None


###########################
# ========= VALUES​ =========
###########################


def p_value(p):
    """value : STRING
    | NUMBER"""

    p[0] = p[1]


def p_values(p):
    "values : values COMMA values"
    p[0] = []
    p[0].extend(p[1])
    p[0].extend(p[3])


###########################
# ===== INSERT VALUES​ ======
###########################


def p_values_end(p):
    "values : value"
    p[0] = [p[1]]


def p_single_values(p):
    "single_values : LPAREN values RPAREN"
    p[0] = p[2]


def p_insert_values(p):
    "insert_values : insert_values COMMA insert_values"
    p[0] = []
    p[0].extend(p[1])
    p[0].extend(p[3])


def p_insert_values_end(p):
    "insert_values : single_values"
    p[0] = [p[1]]


###########################
# ===== Insert Columns​​ =====
###########################


def p_icolumn(p):
    "icolumn : LPAREN columns RPAREN"
    p[0] = p[2]


def p_icolumn_empty(p):
    "icolumn : empty"
    p[0] = None


###########################
# ==== ASSIGNS STATEMENT​​ ===
###########################


def p_assign(p):
    "assign : column EQUAL value"
    p[0] = (p[1], p[3])


def p_assigns(p):
    "assigns : assign COMMA assigns"
    p[0] = [p[1]].extend(p[3])


def p_assigns_end(p):
    "assigns : assign"
    p[0] = [p[1]]
