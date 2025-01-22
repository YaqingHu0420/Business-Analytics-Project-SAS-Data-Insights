/*IMPORT FILE*/
%LET file_path = C:\Users\yhu2\Desktop\Business Analytics Tools-Commercial\ASS\GroupProject-Group3\data;
/*Import order table*/
proc import datafile= "&file_path.\OrderTable.csv" OUT = Orders dbms=csv replace;
run;
/*Import customer table*/
DATA CustomerTable;
	INFILE "&file_path.\CustomerTable.dat" DLM = ';' DSD MISSOVER
    FIRSTOBS = 2;
	INPUT CustomerID:3. Industry:$14. Region:$13. Revenue Employees;
RUN;
/*Import ESG table*/
Proc import datafile = "&file_path.\ESG_scores.csv" out = ESG_scores
	DBMS = csv replace;
run;

/*Create manager table*/
data managers;
input name$ 1-17 Industry$ 18-80;
datalines; 
Tim Tom            Technology
Tim Tom            Finance
Tim Tom            Healthcare
Jonathan Lee Wang  Education
Jonathan Lee Wang  Energy
Jonathan Lee Wang  Manufacturing
Jonathan Lee Wang  Retail
Jonathan Lee Wang  Transportation
;
run;



/*****************************************************************   Part2   *****************************************************************/
/* Merging Orders Table with Customers Table */

data base;
merge Orders CustomerTable;
by CustomerID;
run;

/* Sorting the base by Industry to then merge with Esg_Scores Table and Managers table */
proc sort data=base;
by Industry;
run;

proc sort data=Esg_scores;
by Industry;
run;

proc sort data=managers;
by Industry;
run;

/* Merging base table with Esg_Scores Table and Managers table */
data base;
merge base Esg_scores managers;
by Industry;
run;

/* Check numeric columns with missing values */
proc means data=base n nmiss;
var _numeric_;
run;

/* Check categorical columns with missing values */
proc freq data=base;
tables _character_ / missing;
run;

/* Creating dummy variables for each variable containing missing values */
%macro dummy_variable(data=, var=);
data &data;
set &data;
if missing(&var) then dummy_&var._missing = 1;
else dummy_&var._missing = 0;
run;
%mend dummy_variable;

/*Call the macro for each variable with missing values*/
%dummy_variable(data=base, var=industry);
%dummy_variable(data=base, var=name);
%dummy_variable(data=base, var=revenue);
%dummy_variable(data=base, var=employees);
%dummy_variable(data=base, var=environmental);
%dummy_variable(data=base, var=social);
%dummy_variable(data=base, var=governance);

/* We have missing values in Industry and name categorical variables */
/* Find the mode of Industry */
PROC FREQ DATA=Base NOPRINT;
    TABLES Industry / OUT=Freq_Industry;
RUN;
PROC SORT DATA=Freq_Industry;
    BY DESCENDING COUNT;
RUN;
DATA _NULL_;
    SET Freq_Industry;
    BY DESCENDING COUNT;
    IF _N_ = 1 then call symput("ModeIndustry",Industry); 
RUN;

/* Replace missing values in Industry with the mode */
data base;
set base;
if dummy_industry_missing=1 then Industry= "&ModeIndustry";  
run;

/* Replace missing values in Name with Tim Tom */
data base;
set base;
if dummy_name_missing=1 then name= 'Tim Tom';  
run;

/* We have missing values for Revenue, Employees, Environmental, Social and Governance numeric variables */
/* Find the mean in each varuable */
PROC MEANS NOPRINT DATA = base;
VAR Revenue Employees Environmental Social Governance;
OUTPUT OUT = Avgdata MEAN(Revenue) = AvgRevenue MEAN(Employees)=AvgEmployees 
MEAN(Environmental)=AvgEnvironmental MEAN(Social)=AvgSocial MEAN(Governance)=AvgGovernance;
RUN;
PROC PRINT DATA = Avgdata;
RUN;

/* Store the calculated means in macro variables*/ 
DATA _NULL_;
    SET Avgdata;
    call symput("AvgRevenue",ROUND(AvgRevenue)); 
	call symput ("AvgEmployees",ROUND(AvgEmployees));
	call symput ("AvgEnvironmental",AvgEnvironmental);
    call symput ("AvgSocial",AvgSocial);
	call symput ("AvgGovernance",AvgGovernance);

RUN;

/*Replace missing values with the mean of each numeric variable */ 
data base;
set base;
if dummy_revenue_missing = 1 then revenue="&AvgRevenue";
if dummy_employees_missing = 1 then employees="&AvgEmployees";
if dummy_environmental_missing = 1 then Environmental="&AvgEnvironmental";
if dummy_social_missing = 1 then Social="&AvgSocial";
if dummy_governance_missing = 1 then Governance="&AvgGovernance";
run;

PROC SORT DATA = base;
BY CustomerID;
RUN;


/* Calculate the average amount, the total number of orders and the recency (= time since last order) for each customer */
PROC MEANS NOPRINT DATA = orders MEAN N MAX;
VAR OrderAmount OrderID;
BY CustomerID;
OUTPUT OUT = summarydata MEAN(OrderAmount) = AvgAmount  N(OrderID) = TotalNbrOrders;
RUN;

/*Format OrderDate and calculate recency*/
DATA formatted_orders;
SET orders;
Date_num = input(OrderDate, mmddyy10.);
Year_recency = intck('year', Date_num, today());
days_recency=intck('day', Date_num, today());
RUN;

/*Sort orders by CustomerID and recency*/
PROC SORT DATA= formatted_orders;
BY CustomerID recency;
RUN;

/*Keep only the most recent order for each customer*/
DATA recency (KEEP = CustomerID Orderdate Year_recency days_recency);
    SET formatted_orders;
    BY CustomerID;
    IF FIRST.CustomerID;
RUN;

/*Merge recency data with customer summary data*/
DATA summarydata;
MERGE summarydata recency;
BY CustomerID;
DROP _TYPE_ _FREQ_;
RUN;


/************************************** Creating a basetable by Customer, Granularity is the Customer ****************************************/
data base_to_use;
set base;
run;

/*sort base_to_use and Formatted_orders datasets by OrderID to prepare for merging*/
proc sort data= base_to_use; by OrderID; run;
proc sort data=Formatted_orders; by OrderID; run;

/*Merge base_to_use and Formatted_orders datasets on OrderID*/ 
data basetable (keep=customerID OrderAmount OrderDate Industry Region Revenue name days_recency);
merge base_to_use Formatted_orders;
by OrderID;
run;

/*Sort basetable by CustomerID and days_recency*/
proc sort data=basetable; by CustomerID  days_recency; run;

/*Aggregate OrderAmount and Revenue for each CustomerID */
proc means data=basetable sum noprint;
class CustomerID;
var OrderAmount Revenue;
output out= basetable1 sum(OrderAmount) = total_amount sum(Revenue)=total_Revenue;
run;

/*Merge aggregated data back into basetable*/
data basetable;
merge basetable basetable1;
by CustomerID;
IF FIRST.CustomerID;
run;

data basetable_by_Customer (drop=TYPE FREQ);
set basetable (FIRSTOBS=2);
run;



/*****************************************************************   Part3   *****************************************************************/
/***************************************************************** Insight 1 *****************************************************************/

/*The industry distribution by the number of customers and revenue*/
/*Calculate the total number of customer and total revenue by customer*/
Proc SQL noprint;
	Create table total_customer as
		select count(distinct customerID) as nbr_customer, sum(revenue) as revenue
		from base;
quit;
/*Calculate the total number of customer and total revenue by industry*/
Proc SQL;
	Create table Insights1 as
		select Industry, count(distinct CustomerID)as nbr_customer, count(distinct CustomerID)/(select nbr_customer from total_customer) as pct_nbr_customer format=percent8.2,
		sum(revenue) as total_revenue format = DOLLAR15., sum(revenue)/(select revenue from total_customer) as pct_revenue format = percent8.2
		from Base
		group by Industry
		order by 3 DESC;
Quit;
/*print the output of insight 1 with a title*/
proc data = insights1;
title 'The industry distribution by the number of customers and revenue';
run;

/***************************************************************** Insight 2 *****************************************************************/

/*Average of orders per customer per industry*/
Proc SQL;
	Create table Insights2 as  /*Calculate the number of orders, the number of customers and the average orders per customer by industry*/
		Select Industry,Count(OrderID) as nb_orders,count(distinct CustomerID )as nb_customers,
        Count(OrderID)/count(distinct CustomerID)as avg_order_per_cust format = comma4.2   /*Format the average orders per customer*/
		from Base
		group by  Industry
		order by 4 desc;
Quit;
proc data = insights2;
title 'Average of orders per customer per industry';
run;


/* This table shows the average number of orders made by each customer for each industry */
/* It shows that customers in the Transportation industry make the biggest nb of orders on average */


/***************************************************************** Insight 3 *****************************************************************/

/*Identify which regions have higher or lower ESG, to help align corporate responsibility initiatives with customer profiles*/

/* ESG Scores by Region */
proc sort data=base;
by Region;
run; 
/*Get stadistics of ESG scores for every region*/  
proc means data=base;
class region;
var Environmental Social Governance;
title 'ESG by region';
run;
/*Get the average of ESG score per region*/
proc means data=base noprint;
    class Region;
    var Environmental Social Governance;
    output out=RegionAvg (drop=_TYPE_ _FREQ_) 
		   mean=AvgEnvironmental AvgSocial AvgGovernance;
run;
/* Assign the RiskLevel according to the overall ESG score*/
data base;
    set base;
	LENGTH RiskLevel $10;
    Overall_ESG = mean(of Environmental, Social, Governance);
    if Overall_ESG >= 75 then RiskLevel = 'Low';
    else if Overall_ESG >= 50 then RiskLevel = 'Moderate';
    else RiskLevel = 'High';
run;

/*Clean the table*/
data RegionAvg (keep= CustomerID Region Overall_ESG);
set base;
by CustomerID;
if FIRST.CustomerID;
run;
/*Get the average for each Environmetal, Social and Governance score*/
data Insights3_1;
set RegionAvg;
overall_ESG = mean(of AvgEnvironmental, AvgSocial, AvgGovernance);
AvgEnvironmental = ROUND(AvgEnvironmental,0.01);
AvgSocial = ROUND(AvgSocial,0.01);
AvgGovernance = ROUND(AvgGovernance,0.01);
overall_ESG= ROUND(overall_ESG,0.01);
run;
/*print table*/
proc print data=Insights3_1 (FIRSTOBS = 2);
title 'Average ESG scores per region';
RUN;


/* ESG Score by Industry */
/*Get the overall ESG score by industry and assing the RiskLevel*/
data Insights3_2;
set Esg_scores;
overall_ESG= mean(of Environmental,Social,Governance);
Environmental = ROUND(Environmental,0.01);
Social = ROUND(Social,0.01);
Governance = ROUND(Governance,0.01);
overall_ESG= ROUND(overall_ESG,0.01);
LENGTH RiskLevel $10;
    if Overall_ESG >= 75 then RiskLevel = 'Low';
    else if Overall_ESG >= 50 then RiskLevel = 'Moderate';
    else RiskLevel = 'High';
run;
/*Sort data by overall ESG score*/
proc sort data=Insights3_2;
by overall_ESG;
run;
/*print table*/
proc print data=Insights3_2;
title 'Average ESG score level per industry';
run;


/***************************************************************** Insight 4 *****************************************************************/

/*Customer Segmentation by Revenue and Employee Size */
/*Keep only CustomerID, Revenue and Employees columns from the base table*/
DATA CustomerSegmentation(keep = CustomerID Revenue Employees);
SET base;
BY CustomerID;
IF FIRST.CustomerID;
RUN;
/*Check the mean of revenue and employee size*/
PROC MEANS DATA=CustomerSegmentation NOPRINT;
VAR revenue employees;
OUTPUT OUT = summary_customer_seg;
RUN;
/*Assign new segments based on the mean and generated value of revenue and employee size*/
DATA Insights4;
SET CustomerSegmentation;
LENGTH RevenueSegment $10 EmployeeSegment $10;
if Revenue < 50000000 then RevenueSegment = 'Small';
   else if 50000000 <= Revenue < 100000000 then RevenueSegment = 'Medium';
   else RevenueSegment = 'Large';
if Employees < 500 then EmployeeSegment = 'Small';
   else if 500 <= Employees < 1000 then EmployeeSegment = 'Medium';
   else EmployeeSegment = 'Large';
run;
/*Show the number of customers in each segment, gives an overview of how the customer base is distributed across segments*/
proc freq data=Insights4;
tables RevenueSegment*EmployeeSegment / norow nocol nopercent;
title 'Frequency of Revenue Segments and Employee Segments';
RUN;




/***************************************************************** Insight 5 *****************************************************************/
/* Order Frequency and order amount by Industry and Region */
/*Extract OrderID, OrderAmount, Industry, and Region columns*/
DATA OrderFreq_AvgOrder(keep = OrderID OrderAmount Industry Region);
SET base;
RUN;
/*Calculate the Order frequency and Avarage Order Amount for each Region and Industry*/
proc means data=OrderFreq_AvgOrder NOPRINT;
class Region Industry ;
output out=Insights5 (drop=_TYPE_ _FREQ_)
N(OrderID)=OrderFrequency mean(OrderAmount)=AvgOrderAmount sum(OrderAmount)= TotalRevenue;
run;
/*Start from the 16th observation to skip irrelevant rows and round numerical values to two decimal places*/
data insights5;
set insights5 (FIRSTOBS = 16);
avgorderamount = ROUND(avgorderamount, 1); 
totalrevenue = ROUND(totalrevenue, 1);
RUN;
/*Sort data by Region and descending Order Frequency*/
proc sort data=Insights5;
by region descending OrderFrequency;
run;
/*Print the output*/
PROC PRINT DATA = Insights5 ;
FORMAT avgorderamount totalrevenue DOLLAR20.;
title 'Order Frequency and Avarage Order Amount Per Industry';
RUN;


/************************************************************* Insight 6 **************************************************************************/

/* Top 10 customers with most amounts spent */
/* Getting the amount spent per customer */
proc means data=base noprint;
    class customerID;
    var OrderAmount;
    output out=insights6_1 (drop=_type_ _freq_) sum=TotalOrderAmount n(orderID)=nb_of_orders;
run;

/* Removing the total that is automatically generated */
data insights6_1;
set insights6_1;
if customerID ne .;
run;

/* Sort the two tables to merge */
proc sort data=insights6_1; 
by customerID; 
run;
proc sort data=base; 
by customerID; 
run;
data insights6_1;
    merge insights6_1 (in = left) base(keep=customerID Industry);
    by customerID;
	if left;
run;

/* remove duplicate values */
proc sort data=insights6_1 nodupkey;
    by customerID;
run;

/* sort by total order amount*/
proc sort data=insights6_1;
    by descending totalorderamount;
run;

DATA Insights6_1;
    SET Insights6_1;
	totalorderamount=ROUND(totalorderamount, 1);
RUN;

proc print data=insights6_1  (obs = 10);
FORMAT totalorderamount DOLLAR20.;
title 'Top 10 customers with most amounts spent';
run;


/* Top 10 customers with most number of orders */
/* Getting the amount spent per customer */
proc means data=base noprint;
    class customerID;
    var OrderAmount;
    output out=insights6_2 (drop=_type_ _freq_) 
        n=NumberofOrders 
        sum=TotalOrderAmount;
run;


/* Removing the total that is automatically generated */
data insights6_2;
set insights6_2;
if customerID ne .;
run;

/* Sort the two tables to merge */
proc sort data=insights6_2; 
by customerID; 
run;
proc sort data=base; 
by customerID; 
run;
data insights6_2;
    merge insights6_2 (in = left) base(keep=customerID Industry);
    by customerID;
	if left;
run;

/* remove duplicate values */
proc sort data=insights6_2 nodupkey;
    by customerID;
run;

/* sort by total order amount*/
proc sort data=insights6_2;
    by descending numberoforders;
run;

DATA Insights6_2;
    SET Insights6_2;
	totalorderamount=ROUND(totalorderamount, 1);
RUN;

proc print data=insights6_2  (obs = 10);
FORMAT totalorderamount DOLLAR20.;
title 'Top 10 customers with most amounts spent';
run;



/***************************************************************** Insight 7 *****************************************************************/

/* Top 5 order per industry based on the amount spent */
Data base1;
set base;
run;

proc sort data=base1;
by industry descending OrderAmount;
run;

/* Rank orders within each industry and keep only the top 5 */
data Insights7;
set base1;
by industry;
if first.industry then rank = 1;
else rank + 1;
if rank <= 5;
run;

data Insights7;
    retain industry rank customerID OrderAmount region;
    set Insights7 (keep=industry rank customerID OrderAmount region);
run;

data Insights7;
set Insights7;
orderamount=ROUND(orderamount, 1);
RUN;

proc print data=Insights7;
FORMAT orderamount DOLLAR20.;
title 'Top 5 Orders per Industry Based on Order Amount';
run;



/***************************************************************** Part4 *****************************************************************/
/* top 20 most valuable customers for each account manager based on total amount */
/* sort base table to merge with recency table*/
proc sort data = base;
by customerID;
run;
/* keep only customerID orderamount industry region name year_recency days_recency column*/
data table_sum (keep = customerID orderamount industry region name year_recency days_recency);
merge base recency;
by customerID;
RUN;
data list;
set table_sum;
run;
proc sort data = list;
by name customerID;
run;
/*calculate the total amount for each customer*/
proc means data=list noprint;
    by name;
    class customerID;
    var orderamount;
    output out=summary (drop = _TYPE_ _FREQ_) sum=total_amount;
run;
/*sort summary table and list table to merge information*/
proc sort data=summary;
by descending total_amount;
run;
data summary;
set summary (FIRSTOBS = 3);
run;
proc sort data = summary;
by customerid;
run;
proc sort data = list;
by customerid;
run;
data list_sum;
merge summary (in=left) list(in=right);
by customerid;
if left;
run;
/*remove duplicate values*/
proc sort data=list_sum nodupkey;
by customerID;
run;
/*sort by account manager name and then descending total amount*/
proc sort data=list_sum;
by name descending total_amount;
run;
/*select only the top 20 customers for easch account mamager*/
data top_20;
set list_sum;
by name;
if first.name then rank = 1;
else rank + 1;
if rank <= 20;
run;
/*change the sequence of the column*/
data top_20;
    retain name rank customerID total_amount industry region year_recency;
    set top_20 (keep=name rank customerID total_amount industry region year_recency);
run;

/*change the output format*/
proc print data=top_20;
format total_amount dollar12.2;
run;




/*****************************************************************   Part5   *****************************************************************/
/***************************************************************** Insight 1 *****************************************************************/ 
ODS RTF PATH = 'C:\Users\yhu2\Desktop\Business Analytics Tools-Commercial\ASS\GroupProject-Group3' FILE='Annual_report.rtf';

/* Create macro that takes two years and the aggregation (mean, sum, min, max) as input and returns a report comparing both years */
%macro annualReport(year1=,year2=, type=);
/* Retrieve the year of each column of the base table */
data YearData;
set base;
formatted_date = input(orderdate, mmddyy8.); 
format formatted_date date9.; 
year_value = year(formatted_date); 
run;

/* Retrieve data of the year1 */
data ThisYearData;
set YearData;
where year_value = &year1;
run;

/* Retrieve data of the year2 */
data PrevYearData;
set YearData;
where year_value = &year2;
run;

/* Get the summary of year1 data*/
proc means data=ThisYearData &type noprint;
var Revenue OrderAmount;
output out = SummaryThisYear &type(Revenue)= &type._of_Revenue &type(OrderAmount)=&type._of_OderAmount;
run;

data SummaryThisYear;
set SummaryThisYear;
year=&year1;
run;

/* Get the summary of year2 data*/
proc means data=PrevYearData &type noprint;
var Revenue OrderAmount;
output out = SummaryPrevYear &type(Revenue)= &type._of_Revenue &type(OrderAmount)=&type._of_OderAmount;
run;

data SummaryPrevYear;
set SummaryPrevYear;
year=&year2;
run;

/* Removing type and freq from the report */
data AnnualReport;
set SummaryThisYear (drop= _TYPE_ _FREQ_) SummaryPrevYear (drop= _TYPE_ _FREQ_);
run;

/* Transpose the table to have the years as columns */
proc transpose data=AnnualReport out=AnnualReport ;
	id year; 
    var &type._of_OderAmount &type._of_Revenue ;  
run;

/* Calculate pct_change */
data AnnualReport;
set AnnualReport;
pct_change=(((_&year1-_&year2)/_&year2));
run;

/*Printing the result with the right format */
proc print data=AnnualReport;
FORMAT _&year1 DOLLAR20. _&year2 DOLLAR20. pct_change percent8.2;
title "Annual Report for the year &year1. vs &year2.";
run;

%mend annualReport;

%annualReport(year1=2020, year2=2019, type=sum)
%annualReport(year1=2023, year2=2022, type=sum)


/***************************************************************** Insight 2 *****************************************************************/
%macro rank(compare_var=, type_var=, agg_type = );
/*Use PROC MEANS to calculate the aggregate (sum or mean) of the type_var grouped by compare_var*/
proc means data=base noprint;
class &compare_var; 
var &type_var;       
output out=&agg_type._data (drop=_TYPE_ _FREQ_) &agg_type.(&type_var)=&agg_type._&type_var;
run;

/*Remove the first row of the output dataset*/
data &agg_type._data;
set &agg_type._data (firstobs = 2);
run;

/*Sort the aggregated dataset in descending order of the aggregated variable*/
proc sort data=&agg_type._data out=sorted_data;
by descending &agg_type._&type_var;
run;

/*Rank the sorted values and output the dataset*/
proc rank data=sorted_data out=ranked_base ties=low descending;
var &agg_type._&type_var;
ranks rank;
run;

/*Print the ranked results*/
proc print data=ranked_base;
var &compare_var rank &agg_type._&type_var;
FORMAT &agg_type._&type_var DOLLAR20.;
title "Ranked &compare_var by &agg_type. of &type_var";
run;
%mend rank;
/*call the macro*/
%rank(compare_var= region, type_var= revenue, agg_type = sum)
%rank(compare_var= region, type_var= revenue, agg_type = mean)
%rank(compare_var= industry, type_var= revenue, agg_type = sum)
%rank(compare_var= industry, type_var= revenue, agg_type = mean)

/***************************************************************** Insight 3 *****************************************************************/
%macro annual_info(year=, year2=);
/*Create the annual revenue table with revenue and customer count*/
proc sql;
create table annual_revenue as
select a.industry,
a.total_revenue_&year as total_revenue_&year, 
a.nbr_customer,
b.total_revenue_&year2 as total_revenue_&year2
from
(select industry, sum(revenue) as total_revenue_&year., /* Total revenue for the given year */
count(distinct CustomerID) as nbr_customer /* Number of unique customers */
from Yeardata
where year_value = &year.
group by industry) as a

left join (
select industry, sum(revenue) as total_revenue_&year2.  /* Total revenue for the comparison year */
from Yeardata
where year_value = &year2.  /* Filter for the comparison year */
group by industry) as b
on a.industry=b.industry;

quit;

/* Log the macro processing message */
%put The macro processed data for year &year. &year2.;

/*Calculate percentage change in revenue */
data Annual_revenue;
set Annual_revenue;
pct_change=(((total_revenue_&year. - total_revenue_&year2.)/total_revenue_&year2.));
run;

/*Print the results with proper formatting */
proc print data = Annual_revenue;
FORMAT total_revenue_&year DOLLAR20. total_revenue_&year2 DOLLAR20. pct_change percent8.2;
Title "Comparison of annual revenue and number of customers between &year. and &year2.";
run; 

%mend annual_info;
%annual_info(year=2023, year2=2022)

ODS RTF CLOSE;





