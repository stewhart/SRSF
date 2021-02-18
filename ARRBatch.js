global with sharing class ARRBatch implements Database.Batchable<sObject>, Database.Stateful{
    	global Decimal newARR;
        global Decimal churnARR;
    	global Decimal churnArrLost;
        global Decimal churnArrAdjust;
    	global Decimal startARR;
       	global Decimal working;
    	private String strParameter;
    	global List<Calendar_Account__c> CalendarAccounts = new List<Calendar_Account__c>();
		global list<CurrencyType> cur = new List<CurrencyType>();
    public ARRBatch(String strParam) {
        strParameter = strParam;
        newARR = 0.00;
        churnARR = 0.00;
        churnArrLost = 0.00;
        churnArrAdjust = 0.00;
        startARR = 0.00;
       	working = 0.00;
        //ARR_Churned_Adjustment__c ARR_Churned_Loss__c,
        List<String> splitString = this.strParameter.split('-');
        CalendarAccounts = [SELECT Period_Start__c, Period_End__c, Month__c, Year__c FROM Calendar_Account__c WHERE  Year__c = :splitString[0] AND Month__c = :splitString[1]];
        system.debug(CalendarAccounts);
        cur = [SELECT ISOCode, ConversionRate FROM CurrencyType];
    }
    global Database.QueryLocator start(Database.BatchableContext BC) {
        String query = 'SELECT Amount_New__c, Amount_Before__c, Amount_Increase__c, Next_Increase_date__c, CreatedDate, ARR_Cancelled_Date__c, Recurring_Cancellation_Date__c, Recurring_Cancellation_Type__c, Recurring_Cancelled__c, CurrencyIsoCode FROM Recurring_Increase__c ';
        return Database.getQueryLocator(query);
    }
    global void execute(Database.BatchableContext BC, List<Recurring_Increase__c> scope) {
        
        //List<Calendar_Account__c> CalendarAccounts = new List<Calendar_Account__c>();
        List<Calendar_Account__c> updateCalAccounts = new List<Calendar_Account__c>();
        
        for(Calendar_Account__c calMonths : CalendarAccounts){
            //create a new account for updating
            Calendar_Account__c calAccNew = new Calendar_Account__c();
            calAccNew.id = calMonths.id;
            for(Recurring_Increase__c increases : scope){
                
                if(increases.Amount_Increase__c == null){
                    increases.Amount_Increase__c = 0.00;
                }
                
                // dealing with currency
                if(increases.Amount_Increase__c > 0 && increases.CurrencyIsoCode != 'USD')
                {
                    String RICurrency = increases.CurrencyIsoCode;
                    Integer indexOfCurrency = 0;
                    for(Integer i = 0; i < cur.size(); i++){
                        if(RICurrency == cur[i].IsoCode){
                         increases.Amount_Increase__c = increases.Amount_Increase__c / cur[i].ConversionRate;    
                        }
					}
                    increases.Amount_Increase__c = increases.Amount_Increase__c; 
                }
                 
                if (increases.Recurring_Cancellation_Date__c == null && increases.Recurring_Cancelled__c == true){
                    
                }               
                else {
                // If ARR Started before Month - Existed before period
                if(increases.Next_Increase_date__c < calMonths.Period_Start__c &&  (increases.Recurring_Cancellation_Date__c == null || (increases.Recurring_Cancellation_Date__c >= calMonths.Period_Start__c))){
                    startARR = startARR + increases.Amount_Increase__c;                   
                }
                // Calculate for started During Month - NEW
                if (increases.Next_Increase_date__c >= calMonths.Period_Start__c && increases.Next_Increase_date__c <= calMonths.Period_End__c && increases.Amount_Increase__c > 0 &&
                    (increases.Recurring_Cancellation_Date__c == null || (increases.Recurring_Cancellation_Date__c >= calMonths.Period_Start__c))) {
                  	newARR = newARR + increases.Amount_Increase__c;
                    }
               // calculate churn
                if ((increases.Recurring_Cancellation_Date__c >= calMonths.Period_Start__c && increases.Recurring_Cancellation_Date__c <= calMonths.Period_End__c 
                     	&& increases.Next_Increase_date__c <= calMonths.Period_End__c) 
					|| (increases.Next_Increase_date__c >= calMonths.Period_Start__c && increases.Next_Increase_date__c <= calMonths.Period_End__c && increases.Amount_Increase__c < 0.00)){
                        
                        if(increases.Amount_Increase__c < 0.00 && increases.Recurring_Cancellation_Date__c == null){
                            increases.Amount_Increase__c = increases.Amount_Increase__c * -1;
                        }
                    churnARR = churnARR + increases.Amount_Increase__c;
                    if (increases.Recurring_Cancellation_Type__c == 'LOST'){
                        churnArrLost = churnArrLost + increases.Amount_Increase__c;
                    } else {
                        churnArrAdjust = churnArrAdjust + increases.Amount_Increase__c;
                    }
                }     

           calAccNew.ARR_Start__c = startARR;
           calAccNew.ARR_New__c = newARR;
           calAccNew.ARR_Churned__c = churnARR;
           calAccNew.ARR_Churned_Adjustment__c = churnArrAdjust;
           calAccNew.ARR_Churned_Loss__c = churnArrLost;
           //update calMonths;
                    }
             }
            if(calAccNew !=null){
       			update calAccNew;
  			}
        }  
    }   

    global void finish(Database.BatchableContext BC) {
    }

}
