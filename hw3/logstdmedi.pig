/*standardize medicare data*/

MedData = load 'Medicare.txt' using PigStorage('\t') as (npi, nppes_provider_last_org_name, nppes_provider_first_name, nppes_provider_mi, nppes_credentials, nppes_provider_gender, nppes_entity_code, nppes_provider_street1, nppes_provider_street2, nppes_provider_city, nppes_provider_zip, nppes_provider_state, nppes_provider_country, provider_type, medicare_participation_indicator, place_of_Service, hcpcs_code, hcpcs_description, servicecount, uniqueservices:double, beneday:double, avgmc:double, sdmc:double, avgsub:double, sdsub: double, avgpay:double, sdpay:double);
MedData1 = filter MedData by servicecount != 'servicecount' OR servicecount != '';
--Grouped = group MedData1 all;
LogData = foreach MedData1 generate LOG(uniqueservices+1) as uniqueservices, LOG(beneday+1) as beneday, LOG(avgmc+1) as avgmc, LOG(sdmc+1) as sdmc, LOG(avgsub+1) as avgsub, LOG(sdsub+1) as sdsub, LOG(avgpay+1) as avgpay, LOG(sdpay+1) as sdpay;
GroupedLog = group LogData all;

MinMax = foreach GroupedLog generate MIN(LogData.uniqueservices) as uniquemin, MIN(LogData.beneday) as minday, MIN(LogData.avgmc) as avgmcmin, MIN(LogData.sdmc) as sdmcmin, MIN(LogData.avgsub) as asubmin, MIN(LogData.sdsub) as sdsubmin, MIN(LogData.avgpay) as apaymin, MIN(LogData.sdpay) as sdpaymin, MAX(LogData.uniqueservices) as uniquemax, MAX(LogData.beneday) as maxday, MAX(LogData.avgmc) as avgmcmax, MAX(LogData.sdmc) as sdmcmax, MAX(LogData.avgsub) as asubmax, MAX(LogData.sdsub) as sdsubmax, MAX(LogData.avgpay) as apaymax, MAX(LogData.sdpay) as sdpaymax;

StdData = foreach LogData generate (uniqueservices - MinMax.uniquemin)/(MinMax.uniquemax - MinMax.uniquemin), (beneday - MinMax.minday)/(MinMax.maxday - MinMax.minday), (avgmc - MinMax.avgmcmin)/(MinMax.avgmcmax - MinMax.avgmcmin), (sdmc - MinMax.sdmcmin)/(MinMax.sdmcmax - MinMax.sdmcmin), (avgsub - MinMax.asubmin)/(MinMax.asubmax - MinMax.asubmin), (sdsub - MinMax.sdsubmin)/(MinMax.sdsubmax - MinMax.sdsubmin), (avgpay - MinMax.apaymin)/(MinMax.apaymax - MinMax.apaymin), (sdpay - MinMax.sdpaymin)/(MinMax.sdpaymax - MinMax.sdpaymin);


store StdData into 'LogStdMedicare' using PigStorage();