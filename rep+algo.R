library("DBI")
library("dplyr")
library("inventorize")
library("lubridate")
library("RCurl")
library("readr")
library("stringr")
library("tidyr")
library(RMySQL) 
library(tools)
library(dbplyr)

con<- dbConnect(MySQL(),user='root',password='pass1234',host='localhost',db= 'Shiny_store')


stocks_three_month<- dbGetQuery(con,'SELECT * from shiny_store.mango_stocks WHERE DATE(Date)<= 
(select max(DATE(Date)) from shiny_store.mango_stocks) AND 
DATE(Date)>= (select max(DATE(Date))- interval 90 DAY from shiny_store.mango_stocks);
')


sales_two_years<- dbGetQuery(con,'SELECT * from shiny_store.mango_sales WHERE DATE(Date)<= 
(select max(DATE(Date)) from shiny_store.mango_sales) AND 
DATE(Date)>= (select max(DATE(Date))- interval 730 DAY from shiny_store.mango_sales);
')

orders<- dbGetQuery(con,'SELECT * from shiny_store.mango_orders WHERE DATE(Date)<= 
(select max(DATE(Date)) from shiny_store.mango_orders) AND 
DATE(Date)>= (select max(DATE(Date))- interval 10 DAY from shiny_store.mango_orders);
')

sales_three_month<- dbGetQuery(con,'SELECT * from shiny_store.mango_sales WHERE DATE(Date)<= 
(select max(DATE(Date)) from shiny_store.mango_sales) AND 
DATE(Date)>= (select max(DATE(Date))- interval 90 DAY from shiny_store.mango_sales);
')


#### unique key , every descm size, color, section,subfamily, brand
sales_three_month$Date<- as.Date(sales_three_month$Date)

sales_three_month$label<- paste(sales_three_month$description,
                                sales_three_month$size,
                                sales_three_month$color,
                                sales_three_month$section,
                                sales_three_month$subfamily,
                                sales_three_month$brand,sep = '_')

sales_two_years$Date<- as.Date(sales_two_years$Date)
sales_two_years$label<- paste(sales_two_years$description,
                                sales_two_years$size,
                                sales_two_years$color,
                                sales_two_years$section,
                                sales_two_years$subfamily,
                                sales_two_years$brand,sep = '_')

orders$Date<- as.Date(orders$Date)
orders$label<- paste(orders$description,
                              orders$size,
                              orders$color,
                              orders$section,
                              orders$subfamily,
                              orders$brand,sep = '_')

stocks_three_month$Date<- as.Date(stocks_three_month$Date)
stocks_three_month$label<- paste(stocks_three_month$description,
                              stocks_three_month$size,
                              stocks_three_month$color,
                              stocks_three_month$section,
                              stocks_three_month$subfamily,
                              stocks_three_month$brand,sep = '_')


## sales per day
sales_three_month<- sales_three_month %>% group_by(label,Date) %>% 
  summarise(sales= sum(Qty,na.rm = TRUE),.groups='drop')



#SEASONALITY

sales_two_years$month<- month(sales_two_years$Date,label = TRUE)
sales_two_years$year<- year(sales_two_years$Date)


average_category<- sales_two_years %>% group_by(month,section,subfamily) %>% 
  summarise(sales= sum(Qty,na.rm=TRUE),.groups='drop') %>% group_by(section,subfamily) %>% 
  summarise(average_category= mean(sales,na.rm = TRUE),.groups='drop')


average_monthly<- sales_two_years %>% group_by(year,month,section,subfamily) %>% 
  summarise(sales= sum(Qty,na.rm=TRUE),.groups='drop') %>% group_by(month,section,subfamily) %>% 
  summarise(average_monthly= mean(sales,na.rm = TRUE),.groups='drop')

seasonality<- average_monthly %>% full_join(average_category) %>% 
  mutate(seasonality= average_monthly/average_category) %>% 
  filter(month== month(Sys.Date(),label = TRUE))



### calculate the beginning, ending and recieving inventory

stocks_three_month$beginning<- stocks_three_month$Inventory
stocks_three_month$Inventory<- NULL

a<- stocks_three_month %>% select(label,Date,beginning) %>% group_by(label) %>% 
  nest(data= c(Date,beginning)) %>% 
  mutate(ending= purrr::map(data,function(x)lead(x$beginning))) %>% 
  unnest(cols = c(data,ending))

a<- a %>% full_join(sales_three_month)

## removing NAs and replacing them with zeros
a$sales[is.na(a$sales)]<- 0


dataset<- a
a<- NULL


dataset$recieved<- dataset$ending+ dataset$sales - dataset$beginning
##removing all nas
dataset[,][is.na(dataset)]<-0


dataset<-  dataset %>% mutate(status= case_when( beginning <= 0 & sales== 0 ~'out_of_stock',
                                                 beginning <= 0 & sales== 0 & recieved <= 0~ 'out_of_Stock',
                                                 beginning >0 & sales==0 ~ 'no sales',
                                                 sales >0 ~ 'selling day!',
                                                 sales <0 ~ 'Question marks!'
  
))

product_attributes<- sales_two_years %>% group_by(label) %>% 
  summarise(Cost= mean(Cost,na.rm = TRUE),
            revenue= mean((price_paid/Qty),na.rm = TRUE),
            profit= mean((price_paid/Qty)-Cost,na.rm = TRUE))

dataset <- dataset %>% left_join(product_attributes)


### In transit stock and current stock

leadtime= 10
max_date_requested<- max(orders$Date)
min_date_requested<- min(orders$Date)-10

total_requested<- orders %>% filter(Date %in% c(max_date_requested : min_date_requested)) %>% 
  group_by(label) %>% 
  summarise(total_requested= sum(Qty,na.rm = TRUE),.groups='drop')

current_stock<- dataset %>% filter(Date == max(Date)) %>% select(label,beginning)
last_stock_onhand<- dataset %>% group_by(label) %>% summarise(last_stock_on_hand_date= max(Date[beginning>0]))

names(current_stock)[2]<- 'current_stock'



### clssifactions

for_abc<- dataset %>% group_by(label) %>% 
  summarise(sales= sum(sales,na.rm = TRUE),
            profit= mean(profit,na.rm = TRUE),
            count= n()) %>% arrange(desc(sales))


ABC<- productmix(for_abc$label,for_abc$sales,for_abc$profit)


unique(ABC$product_mix)
ABC<- ABC %>% mutate(drivers= case_when(product_mix== 'A_A'~ 'Volume and Margin Driver',
                                        product_mix== 'C_A' |  product_mix== 'B_A' ~ 'Margin Driver',
                                        product_mix== 'A_B' |product_mix== 'A_C' ~ 'Volume Driver',
                                        product_mix== "C_C" ~ 'Slow Moving',
                                        product_mix=='B_B' |  product_mix=='B_C'|product_mix=='C_B' ~'Regulars'))

names(ABC)[1]<- 'label'
inventory_calculations<- dataset %>% 
  group_by(label) %>% summarise(demand_average= mean(sales[status != 'out_of_stock'],na.rm=TRUE   ),
                                sd= sd (sales,na.rm = TRUE),
                                lt= leadtime,
                                stock_cost= mean(Cost[Cost >0],na.rm = TRUE),
                                days_of_sales= sum(sales >0),
                                days_of_Stock= sum(beginning >0)) %>% 
  mutate(selling_rate_days= days_of_sales/ days_of_Stock) %>% 
  left_join(ABC %>% select(label,sales,revenue,product_mix,drivers))
                                
                                  
names(inventory_calculations)[9]<-'three_month_sales'  
names(inventory_calculations)[10]<-'unit_profit'  


inventory_calculations$estimated_total_profit<- inventory_calculations$three_month_sales * inventory_calculations$unit_profit

inventory_calculations<- inventory_calculations %>% left_join(total_requested) %>% 
  left_join(current_stock) %>% left_join(last_stock_onhand) %>% left_join(product_attributes %>% select(label,revenue)) %>% 
  separate(label,into= c('description',
                                                      'size',
                                                      'color',
                                                      'section',
                                                      'subfamily',
                                                      'brand'),sep='_') %>% left_join(seasonality %>% select(section,subfamily,seasonality))

inventory_calculations$seasonality[is.na(inventory_calculations$seasonality)]<- 1


inventory_calculations$adjusted_average<- inventory_calculations$demand_average * inventory_calculations$seasonality

#calculating demand lead time
inventory_calculations$demand_leadtime<- inventory_calculations$adjusted_average * inventory_calculations$lt


##calculating service levels
inventory_calculations<- inventory_calculations %>% mutate(service_level=
                                                             case_when( drivers== "Volume and Margin Driver" ~0.95,
                                                                        drivers== "Margin Driver" ~0.85,
                                                                        drivers== "Regulars"  ~0.65,
                                                                        drivers== "Slow Moving"    ~0.5,
                                                                        drivers== "Volume Driver"   ~0.85
                                                                        
                                                               
                                                             ))
inventory_calculations$sd[is.na(inventory_calculations$sd)]<- 0
inventory_calculations$sigmadl<- inventory_calculations$sd * sqrt(inventory_calculations$lt)
inventory_calculations$saftey_Stock<- inventory_calculations$sigmadl * qnorm(inventory_calculations$service_level)


inventory_calculations$min<- floor(inventory_calculations$demand_leadtime+ inventory_calculations$saftey_Stock)

inventory_calculations$max<- floor(inventory_calculations$min * 1.6)

#removing all NAs
inventory_calculations[,][is.na(inventory_calculations)]<- 0


reordering<- function( current_stock,total_requested,min,max){
  
  if(current_stock +total_requested <= min){
    max- (current_stock + total_requested)
  } else {
    0
  }
}

inventory_calculations$requested_to_order<- mapply(reordering,
                                                   inventory_calculations$current_stock,
                                                   inventory_calculations$total_requested,
                                                   inventory_calculations$min,
                                                   inventory_calculations$max)

inventory_calculations$current_stock_value<- inventory_calculations$stock_cost * inventory_calculations$current_stock
##otb open to buy
inventory_calculations$ordered_Stock_value<- inventory_calculations$requested_to_order * inventory_calculations$stock_cost


inventory_calculations$three_month_revenue<- inventory_calculations$three_month_sales* inventory_calculations$revenue















inventory_calculations$report_date<- Sys.Date()
dbSendQuery(con,'SET GLOBAL local_infile =true;')



stocks_data<- dbWriteTable(conn = con, name= 'stocks_report',value = inventory_calculations,append=TRUE)

dbDisconnect(con)





