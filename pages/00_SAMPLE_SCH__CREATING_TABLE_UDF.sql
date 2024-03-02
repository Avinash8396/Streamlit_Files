USE ROLE SAMPLE_ROLE;
USE DATABASE NEW_DB;
USE SCHEMA NEW_SCH;



CREATE OR REPLACE TABLE CLONE_TABLE AS CLONE SAMPLE_TABLE;



create function function_name(x integer, y integer)
  returns integer
  language java
  handler='HandlerClass.handlerMethod'
  target_path='@~/HandlerCode.jar'
  as
  $$
      class HandlerClass {
          public static int handlerMethod(int x, int y) {
            return x + y;
          }
      }
  $$;