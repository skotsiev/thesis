import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public class Queries {

    public static HashMap<String, String> hashMap = new HashMap<>();
    static{
        hashMap.put("q1", "SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS SUM_QTY, SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE, AVG(L_QUANTITY) AS AVG_QTY, AVG(L_EXTENDEDPRICE) AS AVG_PRICE, AVG(L_DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE L_SHIPDATE <= DATE '1998-12-01' - INTERVAL '108' DAY GROUP BY L_RETURNFLAG, L_LINESTATUS ORDER BY L_RETURNFLAG, L_LINESTATUS");
        hashMap.put("q2", "SELECT S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT FROM PART, SUPPLIER, PARTSUPP, NATION, REGION WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND P_SIZE = 30 AND P_TYPE LIKE '%STEEL' AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE' AND PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM PARTSUPP, SUPPLIER, NATION, REGION WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE') ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY LIMIT 100");
        hashMap.put("q3", "SELECT L_ORDERKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE, O_ORDERDATE, O_SHIPPRIORITY FROM CUSTOMER, ORDERS, LINEITEM WHERE C_MKTSEGMENT = 'AUTOMOBILE' AND C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND O_ORDERDATE < DATE '1995-03-13' AND L_SHIPDATE > DATE '1995-03-13' GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY ORDER BY REVENUE DESC, O_ORDERDATE LIMIT 10");
        hashMap.put("q4", "SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT FROM ORDERS WHERE O_ORDERDATE >= DATE '1995-01-01' AND O_ORDERDATE < DATE '1995-01-01' + INTERVAL '3' MONTH AND EXISTS (SELECT * FROM LINEITEM WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE) GROUP BY O_ORDERPRIORITY ORDER BY O_ORDERPRIORITY");
        hashMap.put("q5", "SELECT N_NAME, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE FROM CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND L_SUPPKEY = S_SUPPKEY AND C_NATIONKEY = S_NATIONKEY AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'MIDDLE EAST' AND O_ORDERDATE >= DATE '1994-01-01' AND O_ORDERDATE < DATE '1994-01-01' + INTERVAL '1' YEAR GROUP BY N_NAME ORDER BY REVENUE DESC");
        hashMap.put("q6", "SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE FROM LINEITEM WHERE L_SHIPDATE >= DATE '1994-01-01' AND L_SHIPDATE < DATE '1994-01-01' + INTERVAL '1' YEAR AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 AND L_QUANTITY < 24");
        hashMap.put("q7", "SELECT SUPP_NATION, CUST_NATION, L_YEAR, SUM(VOLUME) AS REVENUE FROM ( SELECT N1.N_NAME AS SUPP_NATION, N2.N_NAME AS CUST_NATION, EXTRACT(YEAR FROM L_SHIPDATE) AS L_YEAR, L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME FROM SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION N1, NATION N2 WHERE S_SUPPKEY = L_SUPPKEY AND O_ORDERKEY = L_ORDERKEY AND C_CUSTKEY = O_CUSTKEY AND S_NATIONKEY = N1.N_NATIONKEY AND C_NATIONKEY = N2.N_NATIONKEY AND ((N1.N_NAME = 'JAPAN' AND N2.N_NAME = 'INDIA') OR (N1.N_NAME = 'INDIA' AND N2.N_NAME = 'JAPAN')) AND L_SHIPDATE BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS SHIPPING GROUP BY SUPP_NATION, CUST_NATION, L_YEAR ORDER BY SUPP_NATION, CUST_NATION, L_YEAR");
        hashMap.put("q8", "SELECT O_YEAR, SUM(CASE WHEN NATION = 'INDIA' THEN VOLUME ELSE 0 END) / SUM(VOLUME) AS MKT_SHARE FROM (SELECT EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR,");
        hashMap.put("q9", "SELECT NATION, O_YEAR, SUM(AMOUNT) AS SUM_PROFIT FROM (SELECT N_NAME AS NATION, EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR, L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT FROM PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION WHERE S_SUPPKEY = L_SUPPKEY AND PS_SUPPKEY = L_SUPPKEY AND PS_PARTKEY = L_PARTKEY AND P_PARTKEY = L_PARTKEY AND O_ORDERKEY = L_ORDERKEY AND S_NATIONKEY = N_NATIONKEY AND P_NAME LIKE '%DIM%') AS PROFIT GROUP BY NATION, O_YEAR ORDER BY NATION, O_YEAR DESC");
        hashMap.put("q10", "SELECT C_CUSTKEY, C_NAME, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE, C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT FROM CUSTOMER, ORDERS, LINEITEM, NATION WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND O_ORDERDATE >= DATE '1993-08-01' AND O_ORDERDATE < DATE '1993-08-01' + INTERVAL '3' MONTH AND L_RETURNFLAG = 'R' AND C_NATIONKEY = N_NATIONKEY GROUP BY C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT ORDER BY REVENUE DESC LIMIT 20");
        hashMap.put("q11", "SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE FROM PARTSUPP, SUPPLIER, NATION WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'MOZAMBIQUE' GROUP BY PS_PARTKEY HAVING SUM(PS_SUPPLYCOST * PS_AVAILQTY) > (SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000 FROM PARTSUPP, SUPPLIER, NATION WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'MOZAMBIQUE') ORDER BY VALUE DESC");
        hashMap.put("q12", "SELECT L_SHIPMODE, SUM(CASE WHEN O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT, SUM(CASE WHEN O_ORDERPRIORITY <> '1-URGENT' AND O_ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END) AS LOW_LINE_COUNT FROM ORDERS, LINEITEM WHERE O_ORDERKEY = L_ORDERKEY AND L_SHIPMODE IN ('RAIL', 'FOB') AND L_COMMITDATE < L_RECEIPTDATE AND L_SHIPDATE < L_COMMITDATE AND L_RECEIPTDATE >= DATE '1997-01-01' AND L_RECEIPTDATE < DATE '1997-01-01' + INTERVAL '1' YEAR GROUP BY L_SHIPMODE ORDER BY L_SHIPMODE");
        hashMap.put("q13", "SELECT C_COUNT, COUNT(*) AS CUSTDIST FROM (SELECT C_CUSTKEY, COUNT(O_ORDERKEY) AS C_COUNT FROM CUSTOMER LEFT OUTER JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY AND O_COMMENT NOT LIKE '%PENDING%DEPOSITS%' GROUP BY C_CUSTKEY) C_ORDERS GROUP BY C_COUNT ORDER BY CUSTDIST DESC, C_COUNT DESC");
        hashMap.put("q14", "SELECT 100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT) ELSE 0 END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE FROM LINEITEM, PART WHERE L_PARTKEY = P_PARTKEY AND L_SHIPDATE >= DATE '1996-12-01' AND L_SHIPDATE < DATE '1996-12-01' + INTERVAL '1' MONTH");
        hashMap.put("q15", "CREATE VIEW REVENUE0 (SUPPLIER_NO, TOTAL_REVENUE) AS SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) FROM LINEITEM WHERE L_SHIPDATE >= DATE '1997-07-01' AND L_SHIPDATE < DATE '1997-07-01' + INTERVAL '3' MONTH GROUP BY L_SUPPKEY; SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, TOTAL_REVENUE FROM SUPPLIER, REVENUE0 WHERE S_SUPPKEY = SUPPLIER_NO AND TOTAL_REVENUE = ( SELECT MAX(TOTAL_REVENUE) FROM REVENUE0) ORDER BY S_SUPPKEY; DROP VIEW REVENUE0");
        hashMap.put("q16", "SELECT P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT FROM PARTSUPP, PART WHERE P_PARTKEY = PS_PARTKEY AND P_BRAND <> 'BRAND#34' AND P_TYPE NOT LIKE 'LARGE BRUSHED%' AND P_SIZE IN (48, 19, 12, 4, 41, 7, 21, 39) AND PS_SUPPKEY NOT IN (SELECT S_SUPPKEY FROM SUPPLIER WHERE S_COMMENT LIKE '%CUSTOMER%COMPLAINTS%') GROUP BY P_BRAND, P_TYPE, P_SIZE ORDER BY SUPPLIER_CNT DESC, P_BRAND, P_TYPE, P_SIZE");
        hashMap.put("q17", "SELECT SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY FROM LINEITEM, PART WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'BRAND#44' AND P_CONTAINER = 'WRAP PKG' AND L_QUANTITY < (SELECT 0.2 * AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = P_PARTKEY)");
        hashMap.put("q18", "SELECT C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, SUM(L_QUANTITY) FROM CUSTOMER, ORDERS, LINEITEM WHERE O_ORDERKEY IN (SELECT L_ORDERKEY FROM LINEITEM GROUP BY L_ORDERKEY HAVING SUM(L_QUANTITY) > 314) AND C_CUSTKEY = O_CUSTKEY AND O_ORDERKEY = L_ORDERKEY GROUP BY C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE ORDER BY O_TOTALPRICE DESC, O_ORDERDATE LIMIT 100");
        hashMap.put("q19", "SELECT SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE FROM LINEITEM, PART WHERE (P_PARTKEY = L_PARTKEY AND P_BRAND = 'BRAND#52' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND L_QUANTITY >= 4 AND L_QUANTITY <= 4 + 10 AND P_SIZE BETWEEN 1 AND 5 AND L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON') OR (P_PARTKEY = L_PARTKEY AND P_BRAND = 'BRAND#11' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND L_QUANTITY >= 18 AND L_QUANTITY <= 18 + 10 AND P_SIZE BETWEEN 1 AND 10 AND L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON' ) OR (P_PARTKEY = L_PARTKEY AND P_BRAND = 'BRAND#51' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND L_QUANTITY >= 29 AND L_QUANTITY <= 29 + 10 AND P_SIZE BETWEEN 1 AND 15 AND L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')");
        hashMap.put("q20", "SELECT S_NAME, S_ADDRESS FROM SUPPLIER, NATION WHERE S_SUPPKEY IN ( SELECT PS_SUPPKEY FROM PARTSUPP WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM PART WHERE P_NAME LIKE 'GREEN%') AND PS_AVAILQTY > (SELECT 0.5 * SUM(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = PS_PARTKEY AND L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE >= DATE '1993-01-01' AND L_SHIPDATE < DATE '1993-01-01' + INTERVAL '1' YEAR)) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'ALGERIA' ORDER BY S_NAME");
        hashMap.put("q21", "SELECT S_NAME, COUNT(*) AS NUMWAIT FROM SUPPLIER, LINEITEM L1, ORDERS, NATION WHERE S_SUPPKEY = L1.L_SUPPKEY AND O_ORDERKEY = L1.L_ORDERKEY AND O_ORDERSTATUS = 'F' AND L1.L_RECEIPTDATE > L1.L_COMMITDATE AND EXISTS ( SELECT * FROM LINEITEM L2 WHERE L2.L_ORDERKEY = L1.L_ORDERKEY AND L2.L_SUPPKEY <> L1.L_SUPPKEY) AND NOT EXISTS (SELECT * FROM LINEITEM L3 WHERE L3.L_ORDERKEY = L1.L_ORDERKEY AND L3.L_SUPPKEY <> L1.L_SUPPKEY AND L3.L_RECEIPTDATE > L3.L_COMMITDATE) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'EGYPT' GROUP BY S_NAME ORDER BY NUMWAIT DESC, S_NAME LIMIT 100");
        hashMap.put("q22", "SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL FROM (SELECT SUBSTRING(C_PHONE FROM 1 FOR 2) AS CNTRYCODE, C_ACCTBAL FROM CUSTOMER WHERE SUBSTRING(C_PHONE FROM 1 FOR 2) IN ('20', '40', '22', '30', '39', '42', '21') AND C_ACCTBAL > ( SELECT AVG(C_ACCTBAL) FROM CUSTOMER WHERE C_ACCTBAL > 0.00 AND SUBSTRING(C_PHONE FROM 1 FOR 2) IN ('20', '40', '22', '30', '39', '42', '21')) AND NOT EXISTS ( SELECT * FROM ORDERS WHERE O_CUSTKEY = C_CUSTKEY)) AS CUSTSALE GROUP BY CNTRYCODE ORDER BY CNTRYCODE");
    }

}
