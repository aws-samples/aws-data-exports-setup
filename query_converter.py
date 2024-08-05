#####################################
# Will not work with nested queries #
# save query in test.text           #
#>> python query_converter.py       #
#####################################
product_columns_keep=["product", "product_sku", "product_comment","product_fee_code","product_fee_description","product_from_location","product_from_location_type","product_from_region_code","product_instance_family","product_instance_type","product_instanceSKU","product_location","product_location_type","product_operation","product_pricing_unit","product_product_family","product_region_code","product_servicecode","product_to_location","product_to_location_type","product_to_region_code","product_usagetype"]
#the product columns from CUR that are kept in CUR 2.0

discount_columns_keep=["discount_total_discount", "discount_bundled_discount"]
#the discount columns from CUR that are kept in CUR 2.0

special_columns_keep = product_columns_keep + discount_columns_keep

unchanged_prefix = ("line_item", "identity", "bill", "reservation", "savings_plan", "reservation", "pricing", "split_line_item")

def change_sql():
    reading_file = open("test.txt", "r")

    new_file_content = ""
    colname = True
    for line in reading_file:
        new_line = line.strip()

        new_line = new_line.replace(';', '')
        #import pdb; pdb.set_trace()
        noncom_new_line = new_line.replace(',','') 

        if new_line.lower() == 'from': colname = False
        #check if in cols that stay the same
        if noncom_new_line not in special_columns_keep: #product_columns_keep: 
            #if its not in there check if product fmt
            if new_line.startswith('product'):
                #remove the product_ as now in []
                new_line = new_line[8:] 
                if ',' in new_line:
                    #remove comma and will put at end later
                    new_line = new_line.replace(',','')  

                    #check if in cols and if not assume group by. 
                    if colname == False: new_line = f"product['{new_line}'], " 
                    else: new_line = f"product['{new_line}'] as {noncom_new_line},"
                else : 
                    if colname == False: new_line = f"product['{new_line}']"
                    else: new_line = f"product['{new_line}'] as {noncom_new_line}"

            #do the same for discount
            elif new_line.startswith('discount'):
                #remove the product_ as now in []
                new_line = new_line[9:] 
                if ',' in new_line:
                    #remove comma and will put at end later
                    new_line = new_line.replace(',','')  

                    #check if in cols and if not assume group by. 
                    if colname == False: new_line = f"discount['{new_line}'], " 
                    else: new_line = f"discount['{new_line}'] as {noncom_new_line},"
                else : 
                    if colname == False: new_line = f"discount['{new_line}']"
                    else: new_line = f"discount['{new_line}'] as {noncom_new_line}"

            new_file_content += new_line +"\n"
        else:
            new_file_content += new_line +"\n"
    reading_file.close()

    writing_file = open("test_2.0.txt", "w")
    writing_file.write(new_file_content)
    writing_file.close()

change_sql()