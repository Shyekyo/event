package ideal

/**
  * Created by zhangxiaofan on 2019/6/21.
  */
class CaseClass {
  case class ACCOUNT_TYPE(ACCOUNT_TYPE_ID:Int,ACCOUNT_TYPE_NAME:String,ACCOUNT_REF_PREFIX:String,ACCT_TYPE:String) extends Serializable

  case class ACCT_ITEM(ACCT_ITEM_ID:Int,ITEM_SOURCE_ID:Int,CUST_ID:Int,STATE:Int
                      ,ACCT_ITEM_TYPE_ID:Int,BILLING_CYCLE_ID:Int,CREATE_DATE:Int,
                      PAYMENT_METHOD:Int,REF_NO:String,GEN_FLAG:Int,GEN_ID:Int,
                       COMMENTS:String,PAY_TYPE:String,DEP_ABBR:String,FLAG:Int,SETTLE_DATE:Int,
                      TYPE_ID:Int,BUREAU_ID:Int,APPROVE_FLAG:Int) extends Serializable

  case class ACCT_ITEM_TYPE(ACCT_ITEM_TYPE_ID:Int,ACCT_ITEM_CLASS_ID:Int,ACCOUNT_TYPE_ID:Int,
                            NAME:String,CHARGE_MARK:Int,TOTAL_MARK:Int,ACCOUNT_ITEM_TYPE_CODE:Int,
                            PAY_FLAG:String,MODIFIER:String,UPDATE_TIME:String) extends Serializable
  case class AMOUNT(AMOUNT_ID:Int,ACCOUNT_ITEM_ID:Int,STATE:Int,AMOUNT:Int,CURRENCY:String,COMMENTS:String) extends Serializable

  case class BILLING_CYCLE(BILLING_CYCLE_ID:Int,CYCLE_BEGIN_DATE:Int,CYCLE_END_DATE:Int) extends Serializable

  case class GEN_ITEM(GEN_ID:Int,REF_NO:String,CUST_ID:Int,START_DATE:Int,END_DATE:Int,
                      ACCT_ITEM_TYPE:String,ACCT_FLAG:Int,ACCT_DATE:Int,STATE:Int,
                      PAY_FLAG:String,PAY_TYPE:String,COMMENTS:String,DEP_ABBR:String,
                      PAYMENT_STATE:Int,BUREAU_ID:Int) extends Serializable

  case class STT_OBJECT(CUST_ID:Int,CUST_NAME:String,CUST_TYPE_ID:Integer,CUST_LOCATION:Integer,
                        IS_VIP:Int,PARENT_ID:Int,CUST_CODE:String,STATE:String,EFF_DATE:String,EXP_DATE:String,
                        COUNTRY_ID:Int,SCALE:String,MODIFIER:String,UPDATE_TIME:String,TSP_ID:Integer) extends Serializable

}
