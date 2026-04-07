from generator import (
    build_patient_registry,
    clean_up,
    publish_oltp_order_methods,
    publish_oltp_customers,
    publish_oltp_products,
    publish_oltp_resellers,
    publish_oltp_resellers_csv,
    publish_oltp_transactions,
    publish_preprocessed_resellers_xml,
    create_csv_file,
    create_xml_file,
    set_up_oltp_schema,
)

n = 100
patients = build_patient_registry()

clean_up("data-generate/file_landing/", "xml")
clean_up("data-generate/file_landing/", "csv")
create_csv_file(patients, n)
create_xml_file(patients)
set_up_oltp_schema()
publish_oltp_order_methods()
publish_oltp_customers(patients)
publish_oltp_products()
publish_oltp_resellers()
publish_oltp_resellers_csv()
publish_preprocessed_resellers_xml()
publish_oltp_transactions(patients, n)
