"""
    This file is used to initialise the OLTP and flatfiles
"""

from generator import (
    clean_up,
    publish_oltp_order_methods,
    publish_oltp_customers,
    publish_oltp_products,
    publish_oltp_resellers,
    publish_oltp_transactions,
    create_csv_file,
    create_xml_file,
    set_up_oltp_schema,
)

n = 100
clean_up('data-generate/file_landing/', 'xml')
clean_up('data-generate/file_landing/', 'csv')
set_up_oltp_schema()
publish_oltp_order_methods()
publish_oltp_customers()
publish_oltp_products()
publish_oltp_resellers()
publish_oltp_transactions(n)
create_csv_file(n)
create_xml_file()
