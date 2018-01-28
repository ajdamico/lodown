#' @importFrom utils download.file read.csv unzip getFromNamespace write.csv read.table read.fwf
#' @importFrom stats as.formula vcov coef pf update confint qbeta qt var
#' @importFrom graphics plot rasterImage
NULL

#' locally download and prepare publicly-available microdata
#'
#' get_catalog retrieves a listing of all available extracts for a microdata set
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param catalog \code{data.frame} detailing available microdata extracts
#' @param output_dir directory on your local computer to save the microdata
#' @param ... passed to \code{get_catalog} and \code{lodown_*}
#'
#' @return the microdata you asked for in the folder you specified (or your current working directory)
#'
#' @examples
#'
#' \dontrun{
#'
#' # American Community Survey
#' # download all available microdata
#' lodown( "acs" , output_dir = file.path( path.expand( "~" ) , "ACS" ) )
#' # download only the 2013 files
#' acs_cat <- get_catalog( "acs" , output_dir = file.path( path.expand( "~" ) , "ACS" ) )
#' lodown( "acs" , acs_cat[ acs_cat$year == 2013 , ] )
#'
#' # National Longitudinal Study of Adolescent to Adult Health
#' # download all available microdata
#' lodown( "addhealth" , output_dir = file.path( path.expand( "~" ) , "AddHealth" ) , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the fourth wave of the survey
#' addhealth_cat <- get_catalog( "addhealth" , 
#' 		output_dir = file.path( path.expand( "~" ) , "AddHealth" ) )
#' lodown( "addhealth" , addhealth_cat[ addhealth_cat$wave == "wave iv" , ] , 
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # Area Health Resource File
#' # download all available microdata
#' lodown( "ahrf" , output_dir = file.path( path.expand( "~" ) , "AHRF" ) )
#' # download only the county-level table
#' ahrf_cat <- get_catalog( "ahrf" , output_dir = file.path( path.expand( "~" ) , "AHRF" ) )
#' lodown( "ahrf" , ahrf_cat[ ahrf_cat$directory == "county" , ] )
#'
#' # American Housing Survey
#' # download all available microdata
#' lodown( "ahs" , output_dir = file.path( path.expand( "~" ) , "AHS" ) )
#' # download only the 2013 files
#' ahs_cat <- get_catalog( "ahs" , output_dir = file.path( path.expand( "~" ) , "AHS" ) )
#' lodown( "ahs" , ahs_cat[ ahs_cat$year == 2013 , ] )
#'
#' # American National Election Studies
#' # download all available microdata
#' lodown( "anes" , output_dir = file.path( path.expand( "~" ) , "ANES" ) , 
#' 		your_email = "email@address.com" )
#' # download only the cumulative data file
#' anes_cat <- get_catalog( "anes" , output_dir = file.path( path.expand( "~" ) , "ANES" ) , 
#' 		your_email = "email@address.com" )
#' lodown( "anes" , anes_cat[ grepl( "Cumulative" , anes_cat$directory ) , ] , 
#'		your_email = "email@address.com" )
#'
#' # American Time Use Survey
#' # download all available microdata
#' lodown( "atus" , output_dir = file.path( path.expand( "~" ) , "ATUS" ) )
#' # download only the 2013 files
#' atus_cat <- get_catalog( "atus" , output_dir = file.path( path.expand( "~" ) , "ATUS" ) )
#' lodown( "atus" , atus_cat[ atus_cat$directory == "2013" , ] )
#'
#' # Behavioral Risk Factor Surveillance System
#' # download all available microdata
#' lodown( "brfss" , output_dir = file.path( path.expand( "~" ) , "BRFSS" ) )
#' # download only the 2013 files
#' brfss_cat <- get_catalog( "brfss" , output_dir = file.path( path.expand( "~" ) , "BRFSS" ) )
#' lodown( "brfss" , brfss_cat[ brfss_cat$year == 2013 , ] )
#'
#' # Basic Standalone Medicare Public Use Files
#' # download all available microdata
#' lodown( "bsapuf" , output_dir = file.path( path.expand( "~" ) , "BSAPUF" ) )
#' # download only the prescription drug event tables
#' bsapuf_cat <- get_catalog( "bsapuf" , output_dir = file.path( path.expand( "~" ) , "BSAPUF" ) )
#' lodown( "bsapuf" , bsapuf_cat[ grepl( "PartD" , bsapuf_cat$full_url ) , ] )
#'
#' # Censo Demografico
#' # download all available microdata
#' lodown( "censo" , output_dir = file.path( path.expand( "~" ) , "CENSO" ) )
#' # download only the 2000 and 2010 rio de janeiro extracts
#' censo_cat <- get_catalog( "censo" , output_dir = file.path( path.expand( "~" ) , "CENSO" ) )
#' lodown( "censo" ,  censo_cat[ grepl( "rj" , censo_cat$db_table_prefix ) , ] )
#'
#' # Censo Escolar
#' # download all available microdata
#' lodown( "censoescolar" , output_dir = file.path( path.expand( "~" ) , "CENSOESCOLAR" ) )
#' # download only the 2013 files
#' censoescolar_cat <- get_catalog( "censoescolar" , 
#' 		output_dir = file.path( path.expand( "~" ) , "CENSOESCOLAR" ) )
#' lodown( "censoescolar" , censoescolar_cat[ censoescolar$year == 2013 , ] )
#'
#' # Consumer Expenditure Survey
#' # download all available microdata
#' lodown( "ces" , output_dir = file.path( path.expand( "~" ) , "CES" ) )
#' # download only the 2013 files
#' ces_cat <- get_catalog( "ces" , output_dir = file.path( path.expand( "~" ) , "CES" ) )
#' lodown( "ces" , ces_cat[ ces_cat$year == 2013 , ] )
#'
#' # California Health Interview Survey
#' # download all available microdata
#' lodown( "chis" , output_dir = file.path( path.expand( "~" ) , "CHIS" ) ,
#' 		your_username = "username" , your_password = "password" )
#' # download only the 2013 files
#' chis_cat <- get_catalog( "chis" , output_dir = file.path( path.expand( "~" ) , "CHIS" ) ,
#' 		your_username = "username" , your_password = "password" )
#' lodown( "chis" , chis_cat[ chis_cat$year == 2013 , ]  ,
#' 		your_username = "username" , your_password = "password" )
#'
#' # Current Population Survey - Annual Social and Economic Supplement
#' # download all available microdata
#' lodown( "cpsasec" , output_dir = file.path( path.expand( "~" ) , "CPSASEC" ) )
#' # download only the 2013 files
#' cpsasec_cat <- get_catalog( "cpsasec" , 
#' 		output_dir = file.path( path.expand( "~" ) , "CPSASEC" ) )
#' lodown( "cpsasec" , cps_asec_cat[ cpsasec_cat$year == 2013 , ] )
#'
#' # Current Population Survey - Basic Monthly
#' # download all available microdata
#' lodown( "cpsbasic" , output_dir = file.path( path.expand( "~" ) , "CPSBASIC" ) )
#' # download only the november files
#' cpsbasic_cat <- get_catalog( "cpsbasic" , 
#' 		output_dir = file.path( path.expand( "~" ) , "CPSBASIC" ) )
#' lodown( "cpsbasic" , cpsbasic_cat[ cpsbasic_cat$month == 11 , ] )
#'
#' # Demographic and Health Surveys
#' # download all available microdata
#' lodown( "dhs" , output_dir = file.path( path.expand( "~" ) , "DHS" ) , 
#'		your_email = "email@address.com" , your_password = "password" , your_project = "project" )
#' # download only files after 2010
#' dhs_cat <- get_catalog( "dhs" , output_dir = file.path( path.expand( "~" ) , "DHS" ) ,
#'		your_email = "email@address.com" , your_password = "password" , your_project = "project" )
#' lodown( "dhs" , dhs_cat[ dhs_cat$year > 2010 , ] , 
#'		your_email = "email@address.com" , your_password = "password" , your_project = "project" )
#'
#' # Exame Nacional de Desempenho de Estudantes
#' # download all available microdata
#' lodown( "enade" , output_dir = file.path( path.expand( "~" ) , "ENADE" ) )
#' # download only the 2013 files
#' enade_cat <- get_catalog( "enade" , output_dir = file.path( path.expand( "~" ) , "ENADE" ) )
#' lodown( "enade" , enade_cat[ enade_cat$year == 2013 , ] )
#'
#' # Exame Nacional do Ensino Medio
#' # download all available microdata
#' lodown( "enem" , output_dir = file.path( path.expand( "~" ) , "ENEM" ) )
#' # download only the 2013 files
#' enem_cat <- get_catalog( "enem" , output_dir = file.path( path.expand( "~" ) , "ENEM" ) )
#' lodown( "enem" , enem_cat[ enem_cat$year == 2013 , ] )
#'
#' # European Social Survey
#' # download all available microdata
#' lodown( "ess" , output_dir = file.path( path.expand( "~" ) , "ESS" ) , 
#' 		your_email = "email@address.com" )
#' # download only the integrated files
#' ess_cat <- get_catalog( "ess" , output_dir = file.path( path.expand( "~" ) , "ESS" ) )
#' lodown( "ess" , ess_cat[ ess_cat$directory == 'integrated' , ] , 
#' 		your_email = "email@address.com" )
#'
#' # FDA Adverse Event Reporting System
#' # download all available microdata
#' lodown( "faers" , output_dir = file.path( path.expand( "~" ) , "FAERS" ) )
#' # download only the 2013 files
#' faers_cat <- get_catalog( "faers" , output_dir = file.path( path.expand( "~" ) , "FAERS" ) )
#' lodown( "faers" , faers_cat[ faers_cat == 2013 , ] )
#'
#' # OpenStreetMap Data Extracts
#' # download all available microdata
#' lodown( "geofabrik" , output_dir = file.path( path.expand( "~" ) , "GEOFABRIK" ) )
#' # download only the berlin files
#' geofabrik_cat <- get_catalog( "geofabrik" , 
#' 		output_dir = file.path( path.expand( "~" ) , "GEOFABRIK" ) )
#' lodown( "geofabrik" , geofabrik_cat[ geofabrik_cat$level_three %in% 'Berlin' , ] )
#'
#' # General Social Survey
#' # download all available microdata
#' lodown( "gss" , output_dir = file.path( path.expand( "~" ) , "GSS" ) )
#' # download only the cumulative file
#' gss_cat <- get_catalog( "gss" , output_dir = file.path( path.expand( "~" ) , "GSS" ) )
#' lodown( "gss" , gss_cat[ grepl( "cumulative" , gss_cat$output_filename ) , ] )
#'
#' # Home Mortgage Disclosure Act
#' # download all available microdata
#' lodown( "hmda" , output_dir = file.path( path.expand( "~" ) , "HMDA" ) )
#' # download only the 2013 files
#' hmda_cat <- get_catalog( "hmda" , output_dir = file.path( path.expand( "~" ) , "HMDA" ) )
#' lodown( "hmda" , hmda_cat[ hmda_cat$year == 2013 , ] )
#'
#' # Health and Retirement Study
#' # download all available microdata
#' lodown( "hrs" , output_dir = file.path( path.expand( "~" ) , "HRS" ) , 
#' 		your_username = "username" , your_password = "password" )
#' # download only the rand files
#' hrs_cat <- get_catalog( "hrs" , output_dir = file.path( path.expand( "~" ) , "HRS" ) , 
#' 		your_username = "username" , your_password = "password" )
#' lodown( "hrs" , hrs_cat[ grepl( "rand" , hrs_cat$file_title , ignore.case = TRUE ) , ] , 
#' 		your_username = "username" , your_password = "password" )
#'
#' # Integrated Public Use Microdata Series (IPUMS)
#' # download all available microdata
#' lodown( "IPUMS" , output_dir = file.path( path.expand( "~" ) , "IPUMS" ) , 
#' 		your_email = "email@address.com" , your_password = "password" ,
#'		project = c( "international" , "usa" , "cps" ) )
#' # download only the first extract in your queue
#' ipums_cat <- get_catalog( "ipums" , output_dir = file.path( path.expand( "~" ) , "IPUMS" ) , 
#' 		your_email = "email@address.com" , your_password = "password" ,
#'		project = c( "international" , "usa" , "cps" ) )
#' lodown( "ipums" , ipums_cat[ 1 , ] , 
#' 		your_email = "email@address.com" , your_password = "password" ,
#'		project = c( "international" , "usa" , "cps" ) )
#'
#' # Medical Expenditure Panel Survey
#' # download all available microdata
#' lodown( "meps" , output_dir = file.path( path.expand( "~" ) , "MEPS" ) )
#' # download only the 2013 files
#' meps_cat <- get_catalog( "meps" , output_dir = file.path( path.expand( "~" ) , "MEPS" ) )
#' lodown( "meps" , meps_cat[ grepl( "2013" , meps_cat$year ) , ] )
#'
#' # Medicare Advantage/Part D - Plan Benefits Data
#' # download all available microdata
#' lodown( "mapdbenefits" , output_dir = file.path( path.expand( "~" ) , "MAPDBENEFITS" ) )
#' # download only the 2016 files
#' mapdbenefits_cat <- get_catalog( "mapdbenefits" , output_dir = file.path( path.expand( "~" ) , "MAPDBENEFITS" ) )
#' lodown( "mapdbenefits" , subset( mapdbenefits_cat , year == 2016 ) )
#'
#' # Medicare Advantage/Part D - Contract Plan State County
#' # download all available microdata
#' lodown( "mapdcpsc" , output_dir = file.path( path.expand( "~" ) , "MAPDCPSC" ) )
#' # download only the 2016 files
#' mapdcpsc_cat <- get_catalog( "mapdcpsc" , output_dir = file.path( path.expand( "~" ) , "MAPDCPSC" ) )
#' lodown( "mapdcpsc" , mapdcpsc_cat[ grepl( "2016" , mapdcpsc_cat$year_month ) , ] )
#'
#' # Medicare Advantage/Part D - Crosswalk
#' # download all available microdata
#' lodown( "mapdcrosswalk" , output_dir = file.path( path.expand( "~" ) , "MAPDCROSSWALK" ) )
#' # download only the 2016 file
#' mapdcrosswalk_cat <- get_catalog( "mapdcrosswalk" , output_dir = file.path( path.expand( "~" ) , "MAPDCROSSWALK" ) )
#' lodown( "mapdcrosswalk" , mapdcrosswalk_cat[ grepl( "2016" , mapdcrosswalk_cat$year ) , ] )
#'
#' # Medicare Advantage/Part D - Landscape Files
#' # download all available microdata
#' lodown( "mapdlandscape" , output_dir = file.path( path.expand( "~" ) , "MAPDLANDSCAPE" ) )
#' # download only the 2016 files
#' mapdlandscape_cat <- get_catalog( "mapdlandscape" , output_dir = file.path( path.expand( "~" ) , "MAPDLANDSCAPE" ) )
#' lodown( "mapdlandscape" , mapdlandscape_cat[ grepl( "2016" , mapdlandscape_cat$year ) , ] )
#'
#' # Medicare Advantage/Prescription Drug Plan State/County Penetration Files
#' # download all available microdata
#' lodown( "scpenetration" , output_dir = file.path( path.expand( "~" ) , "SCPENETRATION" ) )
#' # download only the 2016 files
#' scpenetration_cat <- get_catalog( "scpenetration" , output_dir = file.path( path.expand( "~" ) , "SCPENETRATION" ) )
#' lodown( "scpenetration" , scpenetration_cat[ grepl( "2016" , scpenetration_cat$year_month ) , ] )
#'
#' # Multiple Indicator Cluster Surveys
#' # download all available microdata
#' lodown( "mics" , output_dir = file.path( path.expand( "~" ) , "MICS" ) , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the uzbekistan files
#' mics_cat <- get_catalog( "mics" , output_dir = file.path( path.expand( "~" ) , "MICS" ) , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "mics" , mics_cat[ mics_cat$country == 'Uzbekistan' , ] , 
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # Medical Large Claims Experience Study
#' # download all available microdata
#' lodown( "mlces" , output_dir = file.path( path.expand( "~" ) , "MLCES" ) )
#' # download only the 1997 file
#' mlces_cat <- get_catalog( "mlces" , output_dir = file.path( path.expand( "~" ) , "MLCES" ) )
#' lodown( "mlces" , mlces_cat[ grepl( "1997" , mlces_cat$full_url ) , ] )
#'
#' # National Beneficiary Survey
#' # download all available microdata
#' lodown( "nbs" , output_dir = file.path( path.expand( "~" ) , "NBS" ) )
#' # download only the first round
#' nbs_cat <- get_catalog( "nbs" , output_dir = file.path( path.expand( "~" ) , "NBS" ) )
#' lodown( "nbs" , nbs_cat[ nbs_cat$this_round == 1 , ] )
#'
#' # National Crime Victimization Survey
#' # download all available microdata
#' lodown( "ncvs" , output_dir = file.path( path.expand( "~" ) , "NCVS" ) ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the 2013 files
#' ncvs_cat <- get_catalog( "ncvs" , output_dir = file.path( path.expand( "~" ) , "NCVS" ) )
#' lodown( "ncvs" , ncvs_cat[ ncvs_cat$temporalCoverage == 2013 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # National Health and Nutrition Examination Survey
#' # download all available microdata
#' lodown( "nhanes" , output_dir = file.path( path.expand( "~" ) , "NHANES" ) )
#' # download only the 2013-2014 files
#' nhanes_cat <- get_catalog( "nhanes" , output_dir = file.path( path.expand( "~" ) , "NHANES" ) )
#' lodown( "nhanes" , nhanes_cat[ nhanes_cat$years == "2013-2014" , ] )
#'
#' # National Health Interview Survey
#' # download all available microdata
#' lodown( "nhis" , output_dir = file.path( path.expand( "~" ) , "NHIS" ) )
#' # download only the 2013 files
#' nhis_cat <- get_catalog( "nhis" , output_dir = file.path( path.expand( "~" ) , "NHIS" ) )
#' lodown( "nhis" , nhis_cat[ nhis_cat$year == 2013 , ] )
#'
#' # National Household Travel Survey
#' # download all available microdata
#' lodown( "nhts" , output_dir = file.path( path.expand( "~" ) , "NHTS" ) )
#' # download only the 2009 files
#' nhts_cat <- get_catalog( "nhts" , output_dir = file.path( path.expand( "~" ) , "NHTS" ) )
#' lodown( "nhts" , nhts_cat[ nhts_cat$year == 2009 , ] )
#'
#' # National Incident-Based Reporting System
#' # download all available microdata
#' lodown( "nibrs" , output_dir = file.path( path.expand( "~" ) , "NIBRS" ) ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the 2013 files
#' nibrs_cat <- get_catalog( "nibrs" , output_dir = file.path( path.expand( "~" ) , "NIBRS" ) )
#' lodown( "nibrs" , nibrs_cat[ nibrs_cat$temporalCoverage == 2013 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # National Immunization Surveys
#' # download all available microdata
#' lodown( "nis" , output_dir = file.path( path.expand( "~" ) , "NIS" ) )
#' # download only the 2013 files
#' nis_cat <- get_catalog( "nis" , output_dir = file.path( path.expand( "~" ) , "NIS" ) )
#' lodown( "nis" , nis_cat[ nis_cat$year == 2013 , ] )
#'
#' # National Longitudinal Surveys
#' # download all available microdata
#' lodown( "nls" , output_dir = file.path( path.expand( "~" ) , "NLS" ) )
#' # download only the NLSY97
#' nls_cat <- get_catalog( "nls" , output_dir = file.path( path.expand( "~" ) , "NLS" ) )
#' lodown( "nls" , nls_cat[ nls_cat$study_value == "NLSY97" , ] )
#'
#' # National Survey of Children's Health
#' # download all available microdata
#' lodown( "nsch" , output_dir = file.path( path.expand( "~" ) , "NSCH" ) )
#' # download only the 2012 files
#' nsch_cat <- get_catalog( "nsch" , output_dir = file.path( path.expand( "~" ) , "NSCH" ) )
#' lodown( "nsch" , nsch_cat[ nsch_cat$year == 2012 , ] )
#'
#' # National Survey of Family Growth
#' # download all available microdata
#' lodown( "nsfg" , output_dir = file.path( path.expand( "~" ) , "NSFG" ) )
#' # download only the 2013-2015 files
#' nsfg_cat <- get_catalog( "nsfg" , output_dir = file.path( path.expand( "~" ) , "NSFG" ) )
#' lodown( "nsfg" , nsfg_cat[ grepl( "2013_2015" , nsfg_cat$full_url ) , ] )
#'
#' # National Plan and Provider Enumeration System
#' # download all available microdata
#' lodown( "nppes" , output_dir = file.path( path.expand( "~" ) , "NPPES" ) )
#' # edit the database tablename in the first record
#' nppes_cat <- get_catalog( "nppes" , output_dir = file.path( path.expand( "~" ) , "NPPES" ) )
#' nppes_cat[ 1 , 'db_tablename' ] <- 'your_tablename'
#' lodown( "nppes" , nppes_cat )
#'
#' # National Survey of OAA Participants
#' # download all available microdata
#' lodown( "nps" , output_dir = file.path( path.expand( "~" ) , "NPS" ) )
#' # download only the 2013 files
#' nps_cat <- get_catalog( "nps" , output_dir = file.path( path.expand( "~" ) , "NPS" ) )
#' lodown( "nps" , nps_cat[ nps_cat$year == 2013 , ] )
#'
#' # National Survey on Drug Use and Health
#' # download all available microdata
#' lodown( "nsduh" , output_dir = file.path( path.expand( "~" ) , "NSDUH" ) )
#' # download only the 2013 files
#' nsduh_cat <- get_catalog( "nsduh" , output_dir = file.path( path.expand( "~" ) , "NSDUH" ) )
#' lodown( "nsduh" , nsduh_cat[ nsduh_cat$year == 2013 , ] )
#'
#' # National Vital Statistics System
#' # download all available microdata
#' lodown( "nvss" , output_dir = file.path( path.expand( "~" ) , "NVSS" ) )
#' # download only the natality files
#' nvss_cat <- get_catalog( "nvss" , output_dir = file.path( path.expand( "~" ) , "NVSS" ) )
#' lodown( "nvss" , nvss_cat[ nvss_cat$type == 'natality' , ] )
#'
#' # New York City Housing and Vacancy Survey
#' # download all available microdata
#' lodown( "nychvs" , output_dir = file.path( path.expand( "~" ) , "NYCHVS" ) )
#' # download only the 2011 files
#' nychvs_cat <- get_catalog( "nychvs" , output_dir = file.path( path.expand( "~" ) , "NYCHVS" ) )
#' lodown( "nychvs" , nychvs_cat[ nychvs_cat$year == 2011 , ] )
#'
#' # Pew Research Center Surveys
#' # download all available microdata
#' lodown( "pew" , output_dir = file.path( path.expand( "~" ) , "PEW" ) ,
#' 		your_name = "your name" , your_org = "your organization" ,
#' 		your_phone = "555 555 5555" , your_email = "email@address.com" ,
#' 		agree_to_terms = FALSE )
#' # download only the global attitudes and trends surveys
#' pew_cat <- get_catalog( "pew" , output_dir = file.path( path.expand( "~" ) , "PEW" ) )
#' lodown( "pew" , pew_cat[ pew_cat$topic == "Global Attitudes & Trends" , ] ,
#' 		your_name = "your name" , your_org = "your organization" ,
#' 		your_phone = "555 555 5555" , your_email = "email@address.com" ,
#' 		agree_to_terms = FALSE )
#'
#' # Programme for the International Assessment of Adult Competencies
#' # download all available microdata
#' lodown( "piaac" , output_dir = file.path( path.expand( "~" ) , "PIAAC" ) )
#' # download only the italian files
#' piaac_cat <- get_catalog( "piaac" , output_dir = file.path( path.expand( "~" ) , "PIAAC" ) )
#' italian_files <- 
#'		piaac_cat[ grepl( "ita" , basename( piaac_cat$full_url ) , ignore.case = TRUE ) , ]
#' lodown( "piaac" , italian_files )
#'
#' # Progress in International Reading Literacy Study
#' # download all available microdata
#' lodown( "pirls" , output_dir = file.path( path.expand( "~" ) , "PIRLS" ) )
#' # download only the 2011 files
#' pirls_cat <- get_catalog( "pirls" , output_dir = file.path( path.expand( "~" ) , "PIRLS" ) )
#' lodown( "pirls" , pirls_cat[ pirls_cat$year == 2011 , ] )
#'
#' # Programme for International Student Assessment
#' # download all available microdata
#' lodown( "pisa" , output_dir = file.path( path.expand( "~" ) , "PISA" ) )
#' # download only the 2012 files
#' pisa_cat <- get_catalog( "pisa" , output_dir = file.path( path.expand( "~" ) , "PISA" ) )
#' lodown( "pisa" , pisa_cat[ pisa_cat$year == 2012 , ] )
#'
#' # Public Libraries Survey
#' # download all available microdata
#' lodown( "pls" , output_dir = file.path( path.expand( "~" ) , "PLS" ) )
#' # download only the 2013 files
#' pls_cat <- get_catalog( "pls" , output_dir = file.path( path.expand( "~" ) , "PLS" ) )
#' lodown( "pls" , pls_cat[ pls_cat$year == 2013 , ] )
#'
#' # Pesquisa Mensal de Emprego
#' # download all available microdata
#' lodown( "pme" , output_dir = file.path( path.expand( "~" ) , "PME" ) )
#' # download only the 2013 files
#' pme_cat <- get_catalog( "pme" , output_dir = file.path( path.expand( "~" ) , "PME" ) )
#' lodown( "pme" , pme_cat[ pme_cat$year == 2013 , ] )
#'
#' # Pesquisa Nacional de Saude
#' # download all available microdata
#' lodown( "pns" , output_dir = file.path( path.expand( "~" ) , "PNS" ) )
#' # download only the 2013 files
#' pns_cat <- get_catalog( "pns" , output_dir = file.path( path.expand( "~" ) , "PNS" ) )
#' lodown( "pns" , pns_cat[ pns_cat$year == 2013 , ] )
#'
#' # Pesquisa Nacional por Amostra de Domicilios 
#' # download all available microdata
#' lodown( "pnad" , output_dir = file.path( path.expand( "~" ) , "PNAD" ) )
#' # download only the 2013 files
#' pnad_cat <- get_catalog( "pnad" , output_dir = file.path( path.expand( "~" ) , "PNAD" ) )
#' lodown( "pnad" , pnad_cat[ pnad_cat$year == 2013 , ] )
#'
#' # Pesquisa Nacional por Amostra de Domicilios - Continua
#' # download all available microdata
#' lodown( "pnadc" , output_dir = file.path( path.expand( "~" ) , "PNADC" ) )
#' # download only the 2013 files
#' pnadc_cat <- get_catalog( "pnadc" , output_dir = file.path( path.expand( "~" ) , "PNADC" ) )
#' lodown( "pnadc" , pnadc_cat[ pnadc_cat$year == 2013 , ] )
#'
#' # Pesquisa de Orcamentos Familiares
#' # download all available microdata
#' lodown( "pof" , output_dir = file.path( path.expand( "~" ) , "POF" ) )
#' # download only the 2008-2009 files
#' pof_cat <- get_catalog( "pof" , output_dir = file.path( path.expand( "~" ) , "POF" ) )
#' lodown( "pof" , pof_cat[ pof_cat$period == "2008_2009" , ] )
#'
#' # Panel Study of Income Dynamics
#' # download all available microdata
#' lodown( "psid" , output_dir = file.path( path.expand( "~" ) , "PSID" ) , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the cross-year individual file
#' psid_cat <- get_catalog( "psid" , output_dir = file.path( path.expand( "~" ) , "PSID" ) )
#' lodown( "psid" , psid_cat[ grepl( "individual" , psid_cat$table_name ) , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # Sistema de Avaliacao da Educacao Basica
#' # download all available microdata
#' lodown( "saeb" , output_dir = file.path( path.expand( "~" ) , "SAEB" ) )
#' # download only the 2013 files
#' saeb_cat <- get_catalog( "saeb" , output_dir = file.path( path.expand( "~" ) , "SAEB" ) )
#' lodown( "saeb" , saeb_cat[ saeb_cat$year == 2013 , ] )
#'
#' # Survey of Business Owners
#' # download all available microdata
#' lodown( "sbo" , output_dir = file.path( path.expand( "~" ) , "SBO" ) )
#' # edit the database tablename in the first record
#' sbo_cat <- get_catalog( "sbo" , output_dir = file.path( path.expand( "~" ) , "SBO" ) )
#' sbo_cat[ 1 , 'db_tablename' ] <- "your_tablename"
#' lodown( "sbo" , sbo_cat )
#'
#' # Survey of Consumer Finances
#' # download all available microdata
#' lodown( "scf" , output_dir = file.path( path.expand( "~" ) , "SCF" ) )
#' # download only the 2013 files
#' scf_cat <- get_catalog( "scf" , output_dir = file.path( path.expand( "~" ) , "SCF" ) )
#' lodown( "scf" , scf_cat[ scf_cat$year == 2013 , ] )
#'
#' # Surveillance, Epidemiology, and End Results Program
#' # download all available microdata
#' lodown( "seer" , output_dir = file.path( path.expand( "~" ) , "SEER" ) ,
#'		your_username = "username" , your_password = "password" )
#' # viewing the catalog for seer tells you nothing meaningful
#' seer_cat <- get_catalog( "seer" , output_dir = file.path( path.expand( "~" ) , "SEER" ) )
#' lodown( "seer" , seer_cat ,
#'		your_username = "username" , your_password = "password" )
#'
#' # Survey of Health, Ageing and Retirement in Europe
#' # download all available microdata
#' lodown( "share" , output_dir = file.path( path.expand( "~" ) , "SHARE" ) ,
#' 		your_username = "username" , your_password = "password" )
#' # download only the fourth wave
#' share_cat <- get_catalog( "share" , output_dir = file.path( path.expand( "~" ) , "SHARE" ) ,
#' 		your_username = "username" , your_password = "password" )
#' lodown( "share" , share_cat[ grepl( "Wave 4" , share_cat$output_folder ) , ] ,
#' 		your_username = "username" , your_password = "password" )
#'
#' # Survey of Income and Program Participation
#' # download all available microdata
#' lodown( "sipp" , output_dir = file.path( path.expand( "~" ) , "SIPP" ) )
#' # download only the 2008 panel
#' sipp_cat <- get_catalog( "sipp" , output_dir = file.path( path.expand( "~" ) , "SIPP" ) )
#' lodown( "sipp" , sipp_cat[ sipp_cat$panel == 2008 , ] )
#'
#' # Social Security Administration Microdata
#' # download all available microdata
#' lodown( "ssa" , output_dir = file.path( path.expand( "~" ) , "SSA" ) )
#' # download only the earnings data
#' ssa_cat <- get_catalog( "ssa" , output_dir = file.path( path.expand( "~" ) , "SSA" ) )
#' lodown( "ssa" , ssa_cat[ grepl( "earn" , ssa_cat$full_url ) , ] )
#'
#' # Trends in International Mathematics and Science Study
#' # download all available microdata
#' lodown( "timss" , output_dir = file.path( path.expand( "~" ) , "TIMSS" ) )
#' # download only the 2011 files
#' timss_cat <- get_catalog( "timss" , output_dir = file.path( path.expand( "~" ) , "TIMSS" ) )
#' lodown( "timss" , timss_cat[ timss_cat$year == 2011 , ] )
#'
#' # United States Public Use Microdata Sample
#' # download all available microdata
#' lodown( "uspums" , output_dir = file.path( path.expand( "~" ) , "USPUMS" ) )
#' # download only the 2010 census files
#' uspums_cat <- get_catalog( "uspums" , output_dir = file.path( path.expand( "~" ) , "USPUMS" ) )
#' lodown( "uspums" , uspums_cat[ uspums_cat$year == 2010 , ] )
#'
#' # World Values Survey
#' # download all available microdata
#' lodown( "wvs" , output_dir = file.path( path.expand( "~" ) , "WVS" ) )
#' # download only the longitudinal files
#' wvs_cat <- get_catalog( "wvs" , output_dir = file.path( path.expand( "~" ) , "WVS" ) )
#' lodown( "wvs" , wvs_cat[ wvs_cat$wave == -1 , ] )
#'
#' # Youth Risk Behavioral Surveillance System
#' # download all available microdata
#' lodown( "yrbss" , output_dir = file.path( path.expand( "~" ) , "YRBSS" ) )
#' # download only the 2013 files
#' yrbss_cat <- get_catalog( "yrbss" , output_dir = file.path( path.expand( "~" ) , "YRBSS" ) )
#' lodown( "yrbss" , yrbss_cat[ yrbss_cat$year == 2013 , ] )
#'
#'
#'
#' }
#'
#' @export
lodown <-
	function( data_name , catalog = NULL , ... ){

		if( is.null( catalog ) ) catalog <- get_catalog( data_name , ... )

		unique_directories <- unique( c( catalog$unzip_folder , if( 'output_filename' %in% names( catalog ) ) np_dirname( catalog$output_filename ) , if( 'dbfile' %in% names( catalog ) ) np_dirname( catalog$dbfile ) , catalog$output_folder ) )

		for ( this_dir in unique_directories ){
			if( !dir.exists( this_dir ) ){
				tryCatch( { 
					dir.create( this_dir , recursive = TRUE , showWarnings = TRUE ) 
					} , 
					warning = function( w ) stop( "while creating directory " , this_dir , "\n" , conditionMessage( w ) ) 
				)
			}
		}

		catalog$case_count <- NA
		
		load_fun <- getFromNamespace( paste0( "lodown_" , data_name ) , "lodown" )

		cat( paste0( "locally downloading " , data_name , "\r\n\n" ) )

		memory_note <- "\r\n\nlodown is now exiting due to a memory error.\nwindows users: your computing performance would suffer due to disk paging,\nbut you can increase your memory limits with beyond your available hardware with the `?memory.limit` function.\nfor example, you can set the memory ceiling of an R session to 256 GB by typing `memory.limit(256000)`.\r\n\n"
		
		installation_note <- "\r\n\nlodown is now exiting due to an installation error.\r\n\n"
		
		parameter_note <- "\r\n\nlodown is now exiting due to a parameter omission.\r\n\n"
		
		unknown_error_note <- "\r\n\nlodown is now exiting unexpectedly.\nwebsites that host publicly-downloadable microdata change often and sometimes those changes cause this software to break.\nif the error call stack below appears to be a hiccup in your internet connection, then please verify your connectivity and retry the download.\notherwise, please open a new issue at `https://github.com/ajdamico/asdfree/issues` with the contents of this error call stack and also the output of your `sessionInfo()`.\r\n\n"
		
		withCallingHandlers(
			catalog <- load_fun( data_name = data_name , catalog , ... ) , 
			error = 
				function( e ){ 
			
					print( sessionInfo() )
			
					if( grepl( 'cannot allocate vector of size' , e ) ) message( memory_note ) else 
					if( grepl( 'parameter must be specified' , e ) ) message( parameter_note ) else
					if( grepl( 'to install' , e ) ) message( installation_note ) else {
					
						message( unknown_error_note )
					
						print( sys.calls() )
						
					}
					
				}
		)

		cat( paste0( data_name , " local download completed\r\n\n" ) )

		invisible( catalog )

	}

#' @rdname lodown
#' @export
get_catalog <-
	function( data_name , output_dir = getwd() , ... ){

		cat_fun <- getFromNamespace( paste0( "get_catalog_" , data_name ) , "lodown" )

		cat( paste0( "building catalog for " , data_name , "\r\n\n" ) )

		cat_fun( data_name = data_name , output_dir = output_dir , ... )

	}
	
no.na <- function( x , value = FALSE ){ x[ is.na( x ) ] <- value ; x }

unzip_warn_fail <- function( ... ) tryCatch( { unzip( ... ) } , warning = function( w ) stop( conditionMessage( w ) ) )

unarchive_nicely <- 
	function( file_to_unzip , unzip_directory = tempdir() ) {
		file.remove( list.files( file.path( unzip_directory , "archive_unzip" ) , recursive = TRUE , full.names = TRUE ) )
		archive::archive_extract( file_to_unzip , dir = file.path( unzip_directory , "archive_unzip" ) )
		list.files( file.path( unzip_directory , "archive_unzip" ) , recursive = TRUE , full.names = TRUE )
	}

np_dirname <- function( ... ) normalizePath( dirname( ... ) , mustWork = FALSE )
