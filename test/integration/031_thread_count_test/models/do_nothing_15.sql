WITH FUNCTION oracle_sleep(i NUMBER) RETURN NUMBER IS
BEGIN
    DBMS_SESSION.sleep(i);
    RETURN i;
END;
SELECT oracle_sleep(1) as fake FROM DUAL