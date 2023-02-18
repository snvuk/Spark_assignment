from core.util import *


spark = SparkSe()

tran_DF = transactionDF(spark)

def user_DF(spark):
    user_DF = userDF(spark)
    return user_DF

def join_DF():
    join_DF = joinDF(user_DF,tran_DF)
    return join_DF

def cont_loc(count_location):
    cont_loc = count_location(join_DF,"location","product_description")
    return cont_loc

def product(product_Bought):
    product = product_Bought(join_DF,"user_id")
    return product

def total_price():
    total_price = total_spending(join_DF,"userid","price")
    return total_price