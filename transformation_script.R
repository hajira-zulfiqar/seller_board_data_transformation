library(readxl)
library(gsheet)
library(dplyr)
library(data.table)
library(lubridate)
library(googlesheets4)
library(googledrive)
library(plyr)

#getting the master data file
seven_con_master_file <- drive_download("########################", 
                                        overwrite = TRUE)
seven_con_master_data <- readxl::read_excel('##################')

#The weekly new data is to be imported manually for 7th Generation

#change the name of the file below only
new_weekly_data <- read_excel(
  "##########################"
)
#View(new_weekly_data)

#converting the date column name to a term that is not a keyword
#colnames(seven_con_master_data)[colnames(seven_con_master_data) == 'Date'] <- 'DateTimestamp' 
colnames(new_weekly_data)[colnames(new_weekly_data) == 'Date'] <- 'DateTimestamp' 

#assigning correct data type to the date column
#seven_con_master_data$DateTimestamp <- mdy(seven_con_master_data$DateTimestamp)
new_weekly_data$DateTimestamp <- mdy(new_weekly_data$DateTimestamp)


#converting NA to 0 to calculate TACOS and ad_spend
new_weekly_data$SponsoredDisplay[is.na(new_weekly_data$SponsoredDisplay)] = 0
new_weekly_data$SponsoredBrands[is.na(new_weekly_data$SponsoredBrands)] = 0
new_weekly_data$SponsoredProducts[is.na(new_weekly_data$SponsoredProducts)] = 0
new_weekly_data$SponsoredBrandsVideo[is.na(new_weekly_data$SponsoredBrandsVideo)] = 0




#removing rows of data present in new_weekly_data that are already added in the master_raw_data
#filtering for latest date
new_weekly_data_filtered <- new_weekly_data %>%
  filter(DateTimestamp > max(seven_con_master_data$DateTimestamp))

#binding the raw files into a single data frame to ensure row calculations do not contain NA
complete_appended_data <- rbind(seven_con_master_data, new_weekly_data_filtered)

#exporting excel to be added to drive
writexl::write_xlsx(complete_appended_data, "seventh_con_new_master_file")

drive_upload('seventh_con_new_master_file', 
             "~/Reports Automation/7th_Continent/seventh_con_raw_master_file", 
             overwrite = TRUE)

#adding features to the new data file
seventh_con_transformed <- complete_appended_data %>%
  mutate(date_timestamp = DateTimestamp,
         total_sales = SalesOrganic + SalesPPC,
         total_units_sold = UnitsOrganic + UnitsPPC,
         ad_spend = (-1)*(SponsoredProducts + SponsoredDisplay + SponsoredBrands + SponsoredBrandsVideo),
         TACOS = ad_spend/total_sales,
         margin = Margin/100,
         ad_revenue = SalesPPC,
         ppc_units_sold = UnitsPPC,
         ACOS = complete_appended_data$`Real ACOS`/100,
         organic_sales = SalesOrganic,
         organic_sales_percentage = SalesOrganic/total_sales,
         ppc_share_percentage = UnitsPPC/total_units_sold,
         net_profit = NetProfit
  ) %>%
  select(date_timestamp, total_sales, total_units_sold, ad_spend, TACOS, ad_revenue, margin, ppc_units_sold,
         ACOS, organic_sales, organic_sales_percentage, ppc_share_percentage, net_profit)

#rearranging column names
seventh_con_transformed <- seventh_con_transformed[, c("date_timestamp",
                                                       "total_sales",
                                                       "total_units_sold",
                                                       "TACOS",
                                                       "margin",
                                                       "ad_revenue",
                                                       "ad_spend",
                                                       "ppc_units_sold",
                                                       "ACOS",
                                                       "organic_sales",
                                                       "organic_sales_percentage",
                                                       "ppc_share_percentage",
                                                       "net_profit")] # leave the row index blank to keep all rows

#creating aggregations for the new data file
aggregated_transformed_data <- seventh_con_transformed %>%
  #  group_by(date) %>%
  #  arrange(as.Date(date)) %>%
  #  summarise(total_sales = sum(total_sales),
  #            total_units_sold = sum(total_units_sold),
  #            TACOS = mean(TACOS),
  #            margin = mean(margin),
  #            ad_revenue = sum(ad_revenue),
  #            ad_spend = sum(ad_spend),
  #            ppc_units_sold = sum(ppc_units_sold),
  #            ACOS = mean(ACOS),
  #            organic_sales = sum(organic_sales),
#            organic_sales_percentage = mean(organic_sales_percentage),
#            ppc_share_percentage = mean(ppc_share_percentage),
#            net_profit = sum(net_profit)
# ) %>%
#adding differences across days
mutate(sales_difference_value = 
         total_sales - lag(total_sales),
       sales_difference_percentage = 
         (total_sales - lag(total_sales)) / lag(total_sales),
       total_units_difference_value = 
         total_units_sold - lag(total_units_sold),
       total_units_difference_percentage = 
         (total_units_sold - lag(total_units_sold))/lag(total_units_sold),
       tacos_difference_value = 
         TACOS - lag(TACOS),
       tacos_difference_percentage = 
         (TACOS - lag(TACOS))/lag(TACOS),
       margin_difference_value = 
         margin - lag(margin),
       marging_difference_percentage = 
         (margin - lag(margin))/lag(margin),
       ad_revenue_difference_value = 
         ad_revenue - lag(ad_revenue),
       ad_revenue_difference_percentage = 
         (ad_revenue - lag(ad_revenue))/lag(ad_revenue),
       ad_spend_difference_value = 
         ad_spend - lag(ad_spend),
       ad_spend_difference_percentage = 
         (ad_spend - lag(ad_spend))/lag(ad_spend),
       ppc_units_difference_value = 
         ppc_units_sold - lag(ppc_units_sold),
       ppc_units_difference_percentage = 
         (ppc_units_sold - lag(ppc_units_sold))/lag(ppc_units_sold),
       acos_difference_value = 
         ACOS - lag(ACOS),
       acos_difference_percentage = 
         (ACOS - lag(ACOS))/lag(ACOS),
       organic_sale_difference_value = 
         organic_sales - lag(organic_sales),
       organic_sale_difference_percentage = 
         (organic_sales - lag(organic_sales))/lag(organic_sales),
       organic_percent_difference_value = 
         organic_sales_percentage - lag(organic_sales_percentage),
       organic_percent_difference_percentage = 
         (organic_sales_percentage - lag(organic_sales_percentage))/lag(organic_sales_percentage),
       ppc_share_difference_value = 
         ppc_share_percentage - lag(ppc_share_percentage),
       ppc_share_difference_percentage = 
         (ppc_share_percentage - lag(ppc_share_percentage))/lag(ppc_share_percentage),
       net_profit_difference_value = 
         net_profit - lag(net_profit),
       net_profit_difference_percentage = 
         (net_profit - lag(net_profit))/lag(net_profit)
)


#uploading master data file to the source
write_sheet(aggregated_transformed_data, 
            ss = '#########################',
            sheet = '##################')

#creating a copy that is read-friendly for high level users

aggregated_transformed_readable <- aggregated_transformed_data

aggregated_transformed_readable$sales_difference_percentage <- paste(
  round(aggregated_transformed_readable$sales_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$total_units_difference_percentage <- paste(
  round(aggregated_transformed_readable$total_units_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$TACOS <- paste(
  round(aggregated_transformed_readable$TACOS*100,
        2),'%')
aggregated_transformed_readable$tacos_difference_percentage <- paste(
  round(aggregated_transformed_readable$tacos_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$margin <- paste(
  round(aggregated_transformed_readable$margin*100,
        2),'%')
aggregated_transformed_readable$marging_difference_percentage <- paste(
  round(aggregated_transformed_readable$marging_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$ad_revenue_difference_percentage <- paste(
  round(as.numeric(aggregated_transformed_readable$ad_revenue_difference_percentage)*100,
        2),'%')
aggregated_transformed_readable$ad_spend_difference_percentage <- paste(
  round(aggregated_transformed_readable$ad_spend_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$ppc_units_difference_percentage <- paste(
  round(aggregated_transformed_readable$ppc_units_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$ACOS <- paste(
  round(aggregated_transformed_readable$ACOS*100,
        2),'%')
aggregated_transformed_readable$acos_difference_percentage <- paste(
  round(aggregated_transformed_readable$acos_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$organic_sale_difference_percentage <- paste(
  round(aggregated_transformed_readable$organic_sale_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$organic_sales_percentage <- paste(
  round(aggregated_transformed_readable$organic_sales_percentage*100,
        2),'%')
aggregated_transformed_readable$organic_percent_difference_percentage <- paste(
  round(aggregated_transformed_readable$organic_percent_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$ppc_share_percentage <- paste(
  round(aggregated_transformed_readable$ppc_share_percentage*100,
        2),'%')
aggregated_transformed_readable$ppc_share_difference_percentage <- paste(
  round(aggregated_transformed_readable$ppc_share_difference_percentage*100,
        2),'%')
aggregated_transformed_readable$net_profit_difference_percentage <- paste(
  round(aggregated_transformed_readable$net_profit_difference_percentage*100,
        2),'%')

#exporting data to new sheet
write_sheet(aggregated_transformed_readable, 
            ss = '########################',
            sheet = '###########################')


#### Symphonized Data ###

#getting the master data file
sym_master_file <- drive_download("#######################",
                                  overwrite = TRUE)
sym_master_data <- readxl::read_excel('#########################')

#connecting googledrive and downloading latest emailed report
sym_dashboard_totals <- drive_download("###################", overwrite = TRUE)

#getting the dashboardtotals file
sym_new_weekly_data <- read.csv('DashboardTotals.csv')

#converting the date column name to a term that is not a keyword
#colnames(sym_master_data)[colnames(sym_master_data) == 'Date'] <- 'DateTimestamp' 
colnames(sym_new_weekly_data)[colnames(sym_new_weekly_data) == 'Ã¯..Date'] <- 'DateTimestamp' 

#converting NA to 0 to calculate TACOS and ad_spend
sym_new_weekly_data$SponsoredDisplay[is.na(sym_new_weekly_data$SponsoredDisplay)] = 0
sym_new_weekly_data$SponsoredBrands[is.na(sym_new_weekly_data$SponsoredBrands)] = 0
sym_new_weekly_data$SponsoredProducts[is.na(sym_new_weekly_data$SponsoredProducts)] = 0
sym_new_weekly_data$SponsoredBrandsVideo[is.na(sym_new_weekly_data$SponsoredBrandsVideo)] = 0




#removing rows of data present in new_weekly_data that are already added in the master_raw_data
#filtering for latest date
sym_new_weekly_data_filtered <- sym_new_weekly_data %>%
  filter(DateTimestamp > max(sym_master_data$DateTimestamp))

#binding the raw files into a single data frame to ensure row calculations do not contain NA
sym_complete_appended_data <- rbind(sym_master_data, sym_new_weekly_data_filtered)

#exporting excel to be added to drive
writexl::write_xlsx(sym_complete_appended_data, "sym_new_master_file")

drive_upload('##################', 
             "#########################", 
             overwrite = TRUE)

#adding features to the new data file
sym_transformed <- sym_complete_appended_data %>%
  mutate(date_timestamp = DateTimestamp,
         total_sales = SalesOrganic + SalesPPC,
         total_units_sold = UnitsOrganic + UnitsPPC,
         ad_spend = (-1)*(SponsoredProducts + SponsoredDisplay + SponsoredBrands + SponsoredBrandsVideo),
         TACOS = ad_spend/total_sales,
         margin = Margin/100,
         ad_revenue = SalesPPC,
         ppc_units_sold = UnitsPPC,
         ACOS = complete_appended_data$`Real ACOS`/100,
         organic_sales = SalesOrganic,
         organic_sales_percentage = SalesOrganic/total_sales,
         ppc_share_percentage = UnitsPPC/total_units_sold,
         net_profit = NetProfit
  ) %>%
  select(date_timestamp, total_sales, total_units_sold, ad_spend, TACOS, ad_revenue, margin, ppc_units_sold,
         ACOS, organic_sales, organic_sales_percentage, ppc_share_percentage, net_profit)

#rearranging column names
sym_transformed <- sym_transformed[, c("date_timestamp",
                                       "total_sales",
                                       "total_units_sold",
                                       "TACOS",
                                       "margin",
                                       "ad_revenue",
                                       "ad_spend",
                                       "ppc_units_sold",
                                       "ACOS",
                                       "organic_sales",
                                       "organic_sales_percentage",
                                       "ppc_share_percentage",
                                       "net_profit")] # leave the row index blank to keep all rows

#creating aggregations for the new data file
sym_aggregated_transformed_data <- sym_transformed %>%
  #  group_by(date) %>%
  #  arrange(as.Date(date)) %>%
  #  summarise(total_sales = sum(total_sales),
  #            total_units_sold = sum(total_units_sold),
  #            TACOS = mean(TACOS),
  #            margin = mean(margin),
  #            ad_revenue = sum(ad_revenue),
  #            ad_spend = sum(ad_spend),
  #            ppc_units_sold = sum(ppc_units_sold),
  #            ACOS = mean(ACOS),
  #            organic_sales = sum(organic_sales),
#            organic_sales_percentage = mean(organic_sales_percentage),
#            ppc_share_percentage = mean(ppc_share_percentage),
#            net_profit = sum(net_profit)
# ) %>%
#adding differences across days
mutate(sales_difference_value = 
         total_sales - lag(total_sales),
       sales_difference_percentage = 
         (total_sales - lag(total_sales)) / lag(total_sales),
       total_units_difference_value = 
         total_units_sold - lag(total_units_sold),
       total_units_difference_percentage = 
         (total_units_sold - lag(total_units_sold))/lag(total_units_sold),
       tacos_difference_value = 
         TACOS - lag(TACOS),
       tacos_difference_percentage = 
         (TACOS - lag(TACOS))/lag(TACOS),
       margin_difference_value = 
         margin - lag(margin),
       marging_difference_percentage = 
         (margin - lag(margin))/lag(margin),
       ad_revenue_difference_value = 
         ad_revenue - lag(ad_revenue),
       ad_revenue_difference_percentage = 
         (ad_revenue - lag(ad_revenue))/lag(ad_revenue),
       ad_spend_difference_value = 
         ad_spend - lag(ad_spend),
       ad_spend_difference_percentage = 
         (ad_spend - lag(ad_spend))/lag(ad_spend),
       ppc_units_difference_value = 
         ppc_units_sold - lag(ppc_units_sold),
       ppc_units_difference_percentage = 
         (ppc_units_sold - lag(ppc_units_sold))/lag(ppc_units_sold),
       acos_difference_value = 
         ACOS - lag(ACOS),
       acos_difference_percentage = 
         (ACOS - lag(ACOS))/lag(ACOS),
       organic_sale_difference_value = 
         organic_sales - lag(organic_sales),
       organic_sale_difference_percentage = 
         (organic_sales - lag(organic_sales))/lag(organic_sales),
       organic_percent_difference_value = 
         organic_sales_percentage - lag(organic_sales_percentage),
       organic_percent_difference_percentage = 
         (organic_sales_percentage - lag(organic_sales_percentage))/lag(organic_sales_percentage),
       ppc_share_difference_value = 
         ppc_share_percentage - lag(ppc_share_percentage),
       ppc_share_difference_percentage = 
         (ppc_share_percentage - lag(ppc_share_percentage))/lag(ppc_share_percentage),
       net_profit_difference_value = 
         net_profit - lag(net_profit),
       net_profit_difference_percentage = 
         (net_profit - lag(net_profit))/lag(net_profit)
)


#uploading master data file to the source
write_sheet(sym_aggregated_transformed_data, 
            ss = '##################################',
            sheet = '###########################')

#creating a copy that is read-friendly for high level users

sym_aggregated_transformed_readable <- sym_aggregated_transformed_data

sym_aggregated_transformed_readable$sales_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$sales_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$total_units_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$total_units_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$TACOS <- paste(
  round(sym_aggregated_transformed_readable$TACOS*100,
        2),'%')
sym_aggregated_transformed_readable$tacos_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$tacos_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$margin <- paste(
  round(sym_aggregated_transformed_readable$margin*100,
        2),'%')
sym_aggregated_transformed_readable$marging_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$marging_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$ad_revenue_difference_percentage <- paste(
  round(as.numeric(sym_aggregated_transformed_readable$ad_revenue_difference_percentage)*100,
        2),'%')
sym_aggregated_transformed_readable$ad_spend_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$ad_spend_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$ppc_units_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$ppc_units_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$ACOS <- paste(
  round(sym_aggregated_transformed_readable$ACOS*100,
        2),'%')
sym_aggregated_transformed_readable$acos_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$acos_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$organic_sale_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$organic_sale_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$organic_sales_percentage <- paste(
  round(sym_aggregated_transformed_readable$organic_sales_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$organic_percent_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$organic_percent_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$ppc_share_percentage <- paste(
  round(sym_aggregated_transformed_readable$ppc_share_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$ppc_share_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$ppc_share_difference_percentage*100,
        2),'%')
sym_aggregated_transformed_readable$net_profit_difference_percentage <- paste(
  round(sym_aggregated_transformed_readable$net_profit_difference_percentage*100,
        2),'%')

#exporting data to new sheet
write_sheet(sym_aggregated_transformed_readable, 
            ss = '#####################################',
            sheet = '#################################')


#exporting a combined sheet for data studio dashboard
sym_studio_version <- sym_aggregated_transformed_data
sym_studio_version$product <- '###########################'

seventh_con_studio_version <- aggregated_transformed_data
seventh_con_studio_version$date_timestamp <- as.Date(seventh_con_studio_version$date_timestamp, format = "%b %d, %Y")
#seventh_con_studio_version$date_timestamp <- mdy(seventh_con_studio_version$date_timestamp)
seventh_con_studio_version$product <- '###############'

seventh_con_studio_version$date_timestamp <- as.character(seventh_con_studio_version$date_timestamp)
sym_studio_version$date_timestamp <- as.character(sym_studio_version$date_timestamp)

studio_version_data <- rbind(sym_studio_version, seventh_con_studio_version)

#studio_version_data$date_timestamp <- strptime(studio_version_data$date_timestamp, format = "%b %d, %Y")

#exporting data to sheets
write_sheet(studio_version_data, 
            ss = '###############################',
            sheet = '######################')
