COPY PART FROM     '/home/nwatkins/tpch_2_17_0/dbgen/part.csv'     WITH (format CSV, DELIMITER '|');
COPY SUPPLIER FROM '/home/nwatkins/tpch_2_17_0/dbgen/supplier.csv' WITH (format CSV, DELIMITER '|');
COPY PARTSUPP FROM '/home/nwatkins/tpch_2_17_0/dbgen/partsupp.csv' WITH (format CSV, DELIMITER '|');
COPY CUSTOMER FROM '/home/nwatkins/tpch_2_17_0/dbgen/customer.csv' WITH (format CSV, DELIMITER '|');
COPY ORDERS FROM   '/home/nwatkins/tpch_2_17_0/dbgen/orders.csv'   WITH (format CSV, DELIMITER '|');
COPY LINEITEM FROM '/home/nwatkins/tpch_2_17_0/dbgen/lineitem.csv' WITH (format CSV, DELIMITER '|');
COPY NATION FROM   '/home/nwatkins/tpch_2_17_0/dbgen/nation.csv'   WITH (format CSV, DELIMITER '|');
COPY REGION FROM   '/home/nwatkins/tpch_2_17_0/dbgen/region.csv'   WITH (format CSV, DELIMITER '|');
