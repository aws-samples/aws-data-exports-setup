
product_columns_keep=["product", "product_sku", "product_comment","product_fee_code","product_fee_description","product_from_location","product_from_location_type","product_from_region_code","product_instance_family","product_instance_type","product_instanceSKU","product_location","product_location_type","product_operation","product_pricing_unit","product_product_family","product_region_code","product_servicecode","product_to_location","product_to_location_type","product_to_region_code","product_usagetype"]
#the product columns from CUR that are kept in CUR 2.0

discount_columns_keep=["discount_total_discount", "discount_bundled_discount"]
#the discount columns from CUR that are kept in CUR 2.0

special_columns_keep = product_columns_keep + discount_columns_keep

unchanged_prefix = ("line_item", "identity", "bill", "reservation", "savings_plan", "reservation", "pricing", "split_line_item")
    

def old():
      
    for column in column_list:
        if column.startswith(unchanged_prefix):
            CUR2_list.append(column)
            
        elif column in special_columns_keep:
            CUR2_list.append(column) 
            
        elif column.startswith("product"):
            new_col = column.replace("product_", "product.", 1)
            CUR2_list.append(new_col + " AS " + column)
            
        elif column.startswith("cost_category_"):
            new_col = column.replace("cost_category_", "cost_category.", 1)
            CUR2_list.append(new_col + " AS " + column)
            
        elif column.startswith("resource_tags_"):
            new_col = column.replace("resource_tags_", "resource_tags.", 1)
            CUR2_list.append(new_col + " AS " + column)
            
        elif column.startswith("discount_"):
            new_col = column.replace("discount_", "discount.", 1)
            CUR2_list.append(new_col + " AS " + column)

def change_sql():
    reading_file = open("test.txt", "r")

    new_file_content = ""
    
    for line in reading_file:
        new_line = line.strip()
        
        new_line = new_line.replace(';', '')
        if "product" in new_line:    
            new_line = new_line.replace(',','')       
            if new_line not in product_columns_keep:
                new_line = new_line[8:]
                #import pdb; pdb.set_trace()
                if ',' in new_line:
                    
                    new_line = f"product['{new_line}'],"
                else : new_line = f"product['{new_line}']"
                
            new_file_content += new_line +",\n"
        else:
            new_file_content += new_line +"\n"
    reading_file.close()

    writing_file = open("test_2.0.txt", "w")
    writing_file.write(new_file_content)
    writing_file.close()

change_sql()