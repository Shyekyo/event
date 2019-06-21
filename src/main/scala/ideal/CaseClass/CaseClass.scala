package ideal

/**
  * Created by zhangxiaofan on 2019/6/21.
  */
object CaseClass {
  case class ACCOUNT_TYPE(ACCOUNT_TYPE_ID:Integer,ACCOUNT_TYPE_NAME:String,ACCOUNT_REF_PREFIX:String,ACCT_TYPE:String)

  case class ACCT_ITEM(ACCT_ITEM_ID:Integer,ITEM_SOURCE_ID:Integer,CUST_ID:Integer,STATE:Integer
                      ,ACCT_ITEM_TYPE_ID:Integer,BILLING_CYCLE_ID:Integer,CREATE_DATE:Integer,
                      PAYMENT_METHOD:Integer,REF_NO:String,GEN_FLAG:Integer,GEN_ID:Integer,
                       COMMENTS:String,PAY_TYPE:String,DEP_ABBR:String,FLAG:Integer,SETTLE_DATE:Integer,
                      TYPE_ID:Integer,BUREAU_ID:Integer,APPROVE_FLAG:Integer)

  case class ACCT_ITEM_TYPE(ACCT_ITEM_TYPE_ID:Integer,ACCT_ITEM_CLASS_ID:Integer,ACCOUNT_TYPE_ID:Integer,
                            NAME:String,CHARGE_MARK:Integer,TOTAL_MARK:Integer,ACCOUNT_ITEM_TYPE_CODE:Integer,
                            PAY_FLAG:String,MODIFIER:String,UPDATE_TIME:String)
  case class AMOUNT(AMOUNT_ID:Integer,ACCOUNT_ITEM_ID:Integer,STATE:Integer,AMOUNT:Integer,CURRENCY:String,COMMENTS:String)

  case class BILLING_CYCLE(BILLING_CYCLE_ID:Integer,CYCLE_BEGIN_DATE:Integer,CYCLE_END_DATE:Integer)

  case class GEN_ITEM(GEN_ID:Integer,REF_NO:String,CUST_ID:Integer,START_DATE:Integer,END_DATE:Integer,
                      ACCT_ITEM_TYPE:String,ACCT_FLAG:Integer,ACCT_DATE:Integer,STATE:Integer,
                      PAY_FLAG:String,PAY_TYPE:String,COMMENTS:String,DEP_ABBR:String,
                      PAYMENT_STATE:Integer,BUREAU_ID:Integer)

  case class STT_OBJECT(CUST_ID:Integer,CUST_NAME:String,CUST_TYPE_ID:Integer,CUST_LOCATION:Integer,
                        IS_VIP:Integer,PARENT_ID:Integer,CUST_CODE:String,STATE:String,EFF_DATE:String,EXP_DATE:String,
                        COUNTRY_ID:Integer,SCALE:String,MODIFIER:String,UPDATE_TIME:String,TSP_ID:Integer)

}
