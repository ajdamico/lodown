#' @importFrom utils download.file read.csv unzip getFromNamespace write.csv read.table read.fwf
#' @importFrom stats as.formula vcov coef pf update
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
#' lodown( "acs" , output_dir = "C:/My Directory/ACS" )
#' # download only the 2013 files
#' acs <- get_catalog( "acs" , output_dir = "C:/My Directory/ACS" )
#' lodown( "acs" , acs[ acs$year == 2013 , ] )
#'
#' # National Longitudinal Study of Adolescent to Adult Health
#' # download all available microdata
#' lodown( "addhealth" , output_dir = "C:/My Directory/AddHealth" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the fourth wave of the survey
#' addhealth_cat <- get_catalog( "addhealth" , output_dir = "C:/My Directory/AddHealth" )
#' lodown( "addhealth" , addhealth_cat[ addhealth_cat$wave == "wave iv" , ] , 
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # Area Health Resource File
#' # download all available microdata
#' lodown( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' # download only the county-level table
#' ahrf_cat <- get_catalog( "ahrf" , output_dir = "C:/My Directory/AHRF" )
#' lodown( "ahrf" , ahrf_cat[ ahrf_cat$directory == "county" , ] )
#'
#' # American Housing Survey
#' # download all available microdata
#' lodown( "ahs" , output_dir = "C:/My Directory/AHS" )
#' # download only the 2013 files
#' ahs_cat <- get_catalog( "ahs" , output_dir = "C:/My Directory/AHS" )
#' lodown( "ahs" , ahs_cat[ ahs_cat$year == 2013 , ] )
#'
#' # American National Election Studies
#' # download all available microdata
#' lodown( "anes" , output_dir = "C:/My Directory/ANES" , your_email = "email@address.com" )
#' # download only the cumulative data file
#' anes_cat <- get_catalog( "anes" , output_dir = "C:/My Directory/ANES" , 
#' 		your_email = "email@address.com" )
#' lodown( "anes" , anes_cat[ grepl( "Cumulative" , anes_cat$directory ) , ] , 
#'		your_email = "email@address.com" )
#'
#' # American Time Use Survey
#' # download all available microdata
#' lodown( "atus" , output_dir = "C:/My Directory/ATUS" )
#' # download only the 2013 files
#' atus_cat <- get_catalog( "atus" , output_dir = "C:/My Directory/ATUS" )
#' lodown( "atus" , atus_cat[ atus_cat$directory == "2013" , ] )
#'
#' # Behavioral Risk Factor Surveillance System
#' # download all available microdata
#' lodown( "brfss" , output_dir = "C:/My Directory/BRFSS" )
#' # download only the 2013 files
#' brfss_cat <- get_catalog( "brfss" , output_dir = "C:/My Directory/BRFSS" )
#' lodown( "brfss" , brfss_cat[ brfss_cat$year == 2013 , ] )
#'
#' # Basic Standalone Medicare Public Use Files
#' # download all available microdata
#' lodown( "bsapuf" , output_dir = "C:/My Directory/BSAPUF" )
#' # download only the prescription drug event tables
#' bsapuf_cat <- get_catalog( "bsapuf" , output_dir = "C:/My Directory/BSAPUF" )
#' lodown( "bsapuf" , bsapuf_cat[ grepl( "PartD" , bsapuf_cat$full_url ) , ] )
#'
#' # Censo Demografico
#' # download all available microdata
#' lodown( "censo" , output_dir = "C:/My Directory/CENSO" )
#' # download only the 2000 and 2010 rio de janeiro extracts
#' censo_cat <- get_catalog( "censo" , output_dir = "C:/My Directory/CENSO" )
#' lodown( "censo" ,  censo_cat[ grepl( "rj" , censo_cat$db_table_prefix ) , ] )
#'
#' # Censo Escolar
#' # download all available microdata
#' lodown( "censo_escolar" , output_dir = "C:/My Directory/CENSO_ESCOLAR" )
#' # download only the 2013 files
#' censo_escolar_cat <- get_catalog( "censo_escolar" , output_dir = "C:/My Directory/CENSO_ESCOLAR" )
#' lodown( "censo_escolar" , censo_escolar_cat[ censo_escolar$year == 2013 , ] )
#'
#' # Consumer Expenditure Survey
#' # download all available microdata
#' lodown( "ces" , output_dir = "C:/My Directory/CES" )
#' # download only the 2013 files
#' ces_cat <- get_catalog( "ces" , output_dir = "C:/My Directory/CES" )
#' lodown( "ces" , ces_cat[ ces_cat$year == 2013 , ] )
#'
#' # California Health Interview Survey
#' # download all available microdata
#' lodown( "chis" , output_dir = "C:/My Directory/CHIS" ,
#' 		your_username = "username" , your_password = "password" )
#' # download only the 2013 files
#' chis_cat <- get_catalog( "chis" , output_dir = "C:/My Directory/CHIS" ,
#' 		your_username = "username" , your_password = "password" )
#' lodown( "chis" , chis_cat[ chis_cat$year == 2013 , ]  ,
#' 		your_username = "username" , your_password = "password" )
#'
#' # Current Population Survey - Annual Social & Economic Supplement
#' # download all available microdata
#' lodown( "cps_asec" , output_dir = "C:/My Directory/CPS_ASEC" )
#' # download only the 2013 files
#' cps_asec_cat <- get_catalog( "cps_asec" , output_dir = "C:/My Directory/CPS_ASEC" )
#' lodown( "cps_asec" , cps_asec_cat[ cps_asec_cat$year == 2013 , ] )
#'
#' # Current Population Survey - Basic Monthly
#' # download all available microdata
#' lodown( "cps_basic" , output_dir = "C:/My Directory/CPS_BASIC" )
#' # download only the november files
#' cps_basic_cat <- get_catalog( "cps_basic" , output_dir = "C:/My Directory/CPS_BASIC" )
#' lodown( "cps_basic" , cps_basic_cat[ cps_basic_cat$month == 11 , ] )
#'
#' # Demographic & Health Surveys
#' # download all available microdata
#' lodown( "dhs" , output_dir = "C:/My Directory/DHS" , 
#'		your_email = "email@address.com" , your_password = "password" , your_project = "project" )
#' # download only files after 2010
#' dhs_cat <- get_catalog( "dhs" , output_dir = "C:/My Directory/DHS" ,
#'		your_email = "email@address.com" , your_password = "password" , your_project = "project" )
#' lodown( "dhs" , dhs_cat[ dhs_cat$year > 2010 , ] , 
#'		your_email = "email@address.com" , your_password = "password" , your_project = "project" )
#'
#' # European Social Survey
#' # download all available microdata
#' lodown( "ess" , output_dir = "C:/My Directory/ESS" , your_email = "email@address.com" )
#' # download only the integrated files
#' ess_cat <- get_catalog( "ess" , output_dir = "C:/My Directory/ESS" )
#' lodown( "ess" , ess_cat[ ess_cat$directory == 'integrated' , ] , your_email = "email@address.com" )
#'
#' # Exame Nacional do Ensino Medio
#' # download all available microdata
#' lodown( "enem" , output_dir = "C:/My Directory/ENEM" )
#' # download only the 2013 files
#' enem_cat <- get_catalog( "enem" , output_dir = "C:/My Directory/ENEM" )
#' lodown( "enem" , enem_cat[ enem_cat$year == 2013 , ] )
#'
#' # FDA Adverse Event Reporting System
#' # download all available microdata
#' lodown( "faers" , output_dir = "C:/My Directory/FAERS" )
#' # download only the 2013 files
#' faers_cat <- get_catalog( "faers" , output_dir = "C:/My Directory/FAERS" )
#' lodown( "faers" , faers_cat[ faers_cat == 2013 , ] )
#'
#' # OpenStreetMap Data Extracts
#' # download all available microdata
#' lodown( "geofabrik" , output_dir = "C:/My Directory/GEOFABRIK" )
#' # download only the berlin files
#' geofabrik_cat <- get_catalog( "geofabrik" , output_dir = "C:/My Directory/GEOFABRIK" )
#' lodown( "geofabrik" , geofabrik_cat[ geofabrik_cat$level_three %in% 'Berlin' , ] )
#'
#' # General Social Survey
#' # download all available microdata
#' lodown( "gss" , output_dir = "C:/My Directory/GSS" )
#' # download only the cumulative file
#' gss_cat <- get_catalog( "gss" , output_dir = "C:/My Directory/GSS" )
#' lodown( "gss" , gss_cat[ grepl( "cumulative" , gss_cat$output_filename ) , ] )
#'
#' # Home Mortgage Disclosure Act
#' # download all available microdata
#' lodown( "hmda" , output_dir = "C:/My Directory/HMDA" )
#' # download only the 2013 files
#' hmda_cat <- get_catalog( "hmda" , output_dir = "C:/My Directory/HMDA" )
#' lodown( "hmda" , hmda_cat[ hmda_cat$year == 2013 , ] )
#'
#' # Health & Retirement Study
#' # download all available microdata
#' lodown( "hrs" , output_dir = "C:/My Directory/HRS" , 
#' 		your_username = "username" , your_password = "password" )
#' # download only the rand files
#' hrs_cat <- get_catalog( "hrs" , output_dir = "C:/My Directory/HRS" , 
#' 		your_username = "username" , your_password = "password" )
#' lodown( "hrs" , hrs_cat[ grepl( "rand" , hrs_cat$file_title , ignore.case = TRUE ) , ] , 
#' 		your_username = "username" , your_password = "password" )
#'
#' # Medical Expenditure Panel Survey
#' # download all available microdata
#' lodown( "meps" , output_dir = "C:/My Directory/MEPS" )
#' # download only the 2013 files
#' meps_cat <- get_catalog( "meps" , output_dir = "C:/My Directory/MEPS" )
#' lodown( "meps" , meps_cat[ grepl( "2013" , meps_cat$year ) , ] )
#'
#' # Multiple Indicator Cluster Surveys
#' # download all available microdata
#' lodown( "mics" , output_dir = "C:/My Directory/MICS" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the uzbekistan files
#' mics_cat <- get_catalog( "mics" , output_dir = "C:/My Directory/MICS" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' lodown( "mics" , mics_cat[ mics_cat$country == 'Uzbekistan' , ] , 
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # Medical Large Claims Experience Study
#' # download all available microdata
#' lodown( "mlces" , output_dir = "C:/My Directory/MLCES" )
#' # download only the 1997 file
#' mlces_cat <- get_catalog( "mlces" , output_dir = "C:/My Directory/MLCES" )
#' lodown( "mlces" , mlces_cat[ grepl( "1997" , mlces_cat$full_url ) , ] )
#'
#' # National Beneficiary Survey
#' # download all available microdata
#' lodown( "nbs" , output_dir = "C:/My Directory/NBS" )
#' # download only the first round
#' nbs_cat <- get_catalog( "nbs" , output_dir = "C:/My Directory/NBS" )
#' lodown( "nbs" , nbs_cat[ nbs_cat$this_round == 1 , ] )
#'
#' # National Crime Victimization Survey
#' # download all available microdata
#' lodown( "ncvs" , output_dir = "C:/My Directory/NCVS" ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the 2013 files
#' ncvs_cat <- get_catalog( "ncvs" , output_dir = "C:/My Directory/NCVS" )
#' lodown( "ncvs" , ncvs_cat[ ncvs_cat$temporalCoverage == 2013 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # National Health & Nutrition Examination Survey
#' # download all available microdata
#' lodown( "nhanes" , output_dir = "C:/My Directory/NHANES" )
#' # download only the 2013-2014 files
#' nhanes_cat <- get_catalog( "nhanes" , output_dir = "C:/My Directory/NHANES" )
#' lodown( "nhanes" , nhanes_cat[ nhanes_cat$years == "2013-2014" , ] )
#'
#' # National Health Interview Survey
#' # download all available microdata
#' lodown( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' # download only the 2013 files
#' nhis_cat <- get_catalog( "nhis" , output_dir = "C:/My Directory/NHIS" )
#' lodown( "nhis" , nhis_cat[ nhis_cat$year == 2013 , ] )
#'
#' # National Household Travel Survey
#' # download all available microdata
#' lodown( "nhts" , output_dir = "C:/My Directory/NHTS" )
#' # download only the 2009 files
#' nhts_cat <- get_catalog( "nhts" , output_dir = "C:/My Directory/NHTS" )
#' lodown( "nhts" , nhts_cat[ nhts_cat$year == 2009 , ] )
#'
#' # National Incident-Based Reporting System
#' # download all available microdata
#' lodown( "nibrs" , output_dir = "C:/My Directory/NIBRS" ,
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the 2013 files
#' nibrs_cat <- get_catalog( "nibrs" , output_dir = "C:/My Directory/NIBRS" )
#' lodown( "nibrs" , nibrs_cat[ nibrs_cat$temporalCoverage == 2013 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # National Immunization Surveys
#' # download all available microdata
#' lodown( "nis" , output_dir = "C:/My Directory/NIS" )
#' # download only the 2013 files
#' nis_cat <- get_catalog( "nis" , output_dir = "C:/My Directory/NIS" )
#' lodown( "nis" , nis_cat[ nis_cat$year == 2013 , ] )
#'
#' # National Longitudinal Surveys
#' # download all available microdata
#' lodown( "nls" , output_dir = "C:/My Directory/NLS" )
#' # download only the NLSY97
#' nls_cat <- get_catalog( "nls" , output_dir = "C:/My Directory/NLS" )
#' lodown( "nls" , nls_cat[ nls_cat$study_value == "NLSY97" , ] )
#'
#' # National Survey of Children's Health
#' # download all available microdata
#' lodown( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' # download only the 2012 files
#' nsch_cat <- get_catalog( "nsch" , output_dir = "C:/My Directory/NSCH" )
#' lodown( "nsch" , nsch_cat[ nsch_cat$year == 2012 , ] )
#'
#' # National Survey of Family Growth
#' # download all available microdata
#' lodown( "nsfg" , output_dir = "C:/My Directory/NSFG" )
#' # download only the 2013-2015 files
#' nsfg_cat <- get_catalog( "nsfg" , output_dir = "C:/My Directory/NSFG" )
#' lodown( "nsfg" , nsfg_cat[ grepl( "2013_2015" , nsfg_cat$full_url ) , ] )
#'
#' # National Plan & Provider Enumeration System
#' # download all available microdata
#' lodown( "nppes" , output_dir = "C:/My Directory/NPPES" )
#' # edit the database tablename in the first record
#' nppes_cat <- get_catalog( "nppes" , output_dir = "C:/My Directory/NPPES" )
#' nppes_cat[ 1 , 'db_tablename' ] <- 'your_tablename'
#' lodown( "nppes" , nppes_cat )
#'
#' # National Survey of OAA Participants
#' # download all available microdata
#' lodown( "nps" , output_dir = "C:/My Directory/NPS" )
#' # download only the 2013 files
#' nps_cat <- get_catalog( "nps" , output_dir = "C:/My Directory/NPS" )
#' lodown( "nps" , nps_cat[ nps_cat$year == 2013 , ] )
#'
#' # National Survey on Drug Use and Health
#' # download all available microdata
#' lodown( "nsduh" , output_dir = "C:/My Directory/NSDUH" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the 2013 files
#' nsduh_cat <- get_catalog( "nsduh" , output_dir = "C:/My Directory/NSDUH" )
#' lodown( "nsduh" , nsduh_cat[ nsduh_cat$temporalCoverage == 2013 , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # National Vital Statistics System
#' # download all available microdata
#' lodown( "nvss" , output_dir = "C:/My Directory/NVSS" )
#' # download only the natality files
#' nvss_cat <- get_catalog( "nvss" , output_dir = "C:/My Directory/NVSS" )
#' lodown( "nvss" , nvss_cat[ nvss_cat$type == 'natality' , ] )
#'
#' # New York City Housing & Vacancy Survey
#' # download all available microdata
#' lodown( "nychvs" , output_dir = "C:/My Directory/NYCHVS" )
#' # download only the 2011 files
#' nychvs_cat <- get_catalog( "nychvs" , output_dir = "C:/My Directory/NYCHVS" )
#' lodown( "nychvs" , nychvs_cat[ nychvs_cat$year == 2011 , ] )
#'
#' # Pew Research Center Surveys
#' # download all available microdata
#' lodown( "pew" , output_dir = "C:/My Directory/PEW" ,
#' 		your_name = "your name" , your_org = "your organization" ,
#' 		your_phone = "555 555 5555" , your_email = "email@address.com" ,
#' 		agree_to_terms = FALSE )
#' # download only the global attitudes & trends surveys
#' pew_cat <- get_catalog( "pew" , output_dir = "C:/My Directory/PEW" )
#' lodown( "pew" , pew_cat[ pew_cat$topic == "Global Attitudes & Trends" , ] ,
#' 		your_name = "your name" , your_org = "your organization" ,
#' 		your_phone = "555 555 5555" , your_email = "email@address.com" ,
#' 		agree_to_terms = FALSE )
#'
#' # Programme for the International Assessment of Adult Competencies
#' # download all available microdata
#' lodown( "piaac" , output_dir = "C:/My Directory/PIAAC" )
#' # download only the italian files
#' piaac_cat <- get_catalog( "piaac" , output_dir = "C:/My Directory/PIAAC" )
#' italian_files <- 
#'		piaac_cat[ grepl( "ita" , basename( piaac_cat$full_url ) , ignore.case = TRUE ) , ]
#' lodown( "piaac" , italian_files )
#'
#' # Progress in International Reading Literacy Study
#' # download all available microdata
#' lodown( "pirls" , output_dir = "C:/My Directory/PIRLS" )
#' # download only the 2011 files
#' pirls_cat <- get_catalog( "pirls" , output_dir = "C:/My Directory/PIRLS" )
#' lodown( "pirls" , pirls_cat[ pirls_cat$year == 2011 , ] )
#'
#' # Programme for International Student Assessment
#' # download all available microdata
#' lodown( "pisa" , output_dir = "C:/My Directory/PISA" )
#' # download only the 2012 files
#' pisa_cat <- get_catalog( "pisa" , output_dir = "C:/My Directory/PISA" )
#' lodown( "pisa" , pisa_cat[ pisa_cat$year == 2012 , ] )
#'
#' # Public Libraries Survey
#' # download all available microdata
#' lodown( "pls" , output_dir = "C:/My Directory/PLS" )
#' # download only the 2013 files
#' pls_cat <- get_catalog( "pls" , output_dir = "C:/My Directory/PLS" )
#' lodown( "pls" , pls_cat[ pls_cat$year == 2013 , ] )
#'
#' # Pesquisa Mensal de Emprego
#' # download all available microdata
#' lodown( "pme" , output_dir = "C:/My Directory/PME" )
#' # download only the 2013 files
#' pme_cat <- get_catalog( "pme" , output_dir = "C:/My Directory/PME" )
#' lodown( "pme" , pme_cat[ pme_cat$year == 2013 , ] )
#'
#' # Pesquisa Nacional de Saude
#' # download all available microdata
#' lodown( "pns" , output_dir = "C:/My Directory/PNS" )
#' # download only the 2013 files
#' pns_cat <- get_catalog( "pns" , output_dir = "C:/My Directory/PNS" )
#' lodown( "pns" , pns_cat[ pns_cat$year == 2013 , ] )
#'
#' # Pesquisa Nacional por Amostra de Domicilios 
#' # download all available microdata
#' lodown( "pnad" , output_dir = "C:/My Directory/PNAD" )
#' # download only the 2013 files
#' pnad_cat <- get_catalog( "pnad" , output_dir = "C:/My Directory/PNAD" )
#' lodown( "pnad" , pnad_cat[ pnad_cat$year == 2013 , ] )
#'
#' # Pesquisa Nacional por Amostra de Domicilios - Continua
#' # download all available microdata
#' lodown( "pnadc" , output_dir = "C:/My Directory/PNADC" )
#' # download only the 2013 files
#' pnadc_cat <- get_catalog( "pnadc" , output_dir = "C:/My Directory/PNADC" )
#' lodown( "pnadc" , pnadc_cat[ pnadc_cat$year == 2013 , ] )
#'
#' # Pesquisa de Orcamentos Familiares
#' # download all available microdata
#' lodown( "pof" , output_dir = "C:/My Directory/POF" )
#' # download only the 2008-2009 files
#' pof_cat <- get_catalog( "pof" , output_dir = "C:/My Directory/POF" )
#' lodown( "pof" , pof_cat[ pof_cat$period == "2008_2009" , ] )
#'
#' # Panel Study of Income Dynamics
#' # download all available microdata
#' lodown( "psid" , output_dir = "C:/My Directory/PSID" , 
#' 		your_email = "email@address.com" , your_password = "password" )
#' # download only the cross-year individual file
#' psid_cat <- get_catalog( "psid" , output_dir = "C:/My Directory/PSID" )
#' lodown( "psid" , psid_cat[ grepl( "individual" , psid_cat$table_name ) , ] ,
#' 		your_email = "email@address.com" , your_password = "password" )
#'
#' # Sistema de Avaliacao da Educacao Basica
#' # download all available microdata
#' lodown( "saeb" , output_dir = "C:/My Directory/SAEB" )
#' # download only the 2013 files
#' saeb_cat <- get_catalog( "saeb" , output_dir = "C:/My Directory/SAEB" )
#' lodown( "saeb" , saeb_cat[ saeb_cat$year == 2013 , ] )
#'
#' # Survey of Business Owners
#' # download all available microdata
#' lodown( "sbo" , output_dir = "C:/My Directory/SBO" )
#' # edit the database tablename in the first record
#' sbo_cat <- get_catalog( "sbo" , output_dir = "C:/My Directory/SBO" )
#' sbo_cat[ 1 , 'db_tablename' ] <- "your_tablename"
#' lodown( "sbo" , sbo_cat )
#'
#' # Survey of Consumer Finances
#' # download all available microdata
#' lodown( "scf" , output_dir = "C:/My Directory/SCF" )
#' # download only the 2013 files
#' scf_cat <- get_catalog( "scf" , output_dir = "C:/My Directory/SCF" )
#' lodown( "scf" , scf_cat[ scf_cat$year == 2013 , ] )
#'
#' # Surveillance, Epidemiology, and End Results Program
#' # download all available microdata
#' lodown( "seer" , output_dir = "C:/My Directory/SEER" ,
#'		your_username = "username" , your_password = "password" )
#' # viewing the catalog for seer tells you nothing meaningful
#' seer_cat <- get_catalog( "seer" , output_dir = "C:/My Directory/SEER" )
#' lodown( "seer" , seer_cat ,
#'		your_username = "username" , your_password = "password" )
#'
#' # Survey of Health, Ageing and Retirement in Europe
#' # download all available microdata
#' lodown( "share" , output_dir = "C:/My Directory/SHARE" ,
#' 		your_username = "username" , your_password = "password" )
#' # download only the fourth wave
#' share_cat <- get_catalog( "share" , output_dir = "C:/My Directory/SHARE" ,
#' 		your_username = "username" , your_password = "password" )
#' lodown( "share" , share_cat[ grepl( "Wave 4" , share_cat$output_folder ) , ] ,
#' 		your_username = "username" , your_password = "password" )
#'
#' # Survey of Income & Program Participation
#' # download all available microdata
#' lodown( "sipp" , output_dir = "C:/My Directory/SIPP" )
#' # download only the 2008 panel
#' sipp_cat <- get_catalog( "sipp" , output_dir = "C:/My Directory/SIPP" )
#' lodown( "sipp" , sipp_cat[ sipp_cat$panel == 2008 , ] )
#'
#' # Social Security Administration Microdata
#' # download all available microdata
#' lodown( "ssa" , output_dir = "C:/My Directory/SSA" )
#' # download only the earnings data
#' ssa_cat <- get_catalog( "ssa" , output_dir = "C:/My Directory/SSA" )
#' lodown( "ssa" , ssa_cat[ grepl( "earn" , ssa_cat$full_url ) , ] )
#'
#' # Trends in International Mathematics and Science Study
#' # download all available microdata
#' lodown( "timss" , output_dir = "C:/My Directory/TIMSS" )
#' # download only the 2011 files
#' timss_cat <- get_catalog( "timss" , output_dir = "C:/My Directory/TIMSS" )
#' lodown( "timss" , timss_cat[ timss_cat$year == 2011 , ] )
#'
#' # United States Public Use Microdata Sample
#' # download all available microdata
#' lodown( "uspums" , output_dir = "C:/My Directory/USPUMS" )
#' # download only the 2010 census files
#' uspums_cat <- get_catalog( "uspums" , output_dir = "C:/My Directory/USPUMS" )
#' lodown( "uspums" , uspums_cat[ uspums_cat$year == 2010 , ] )
#'
#' # World Values Survey
#' # download all available microdata
#' lodown( "wvs" , output_dir = "C:/My Directory/WVS" )
#' # download only the longitudinal files
#' wvs_cat <- get_catalog( "wvs" , output_dir = "C:/My Directory/WVS" )
#' lodown( "wvs" , wvs_cat[ wvs_cat$wave == -1 , ] )
#'
#' # Youth Risk Behavioral Surveillance System
#' # download all available microdata
#' lodown( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
#' # download only the 2013 files
#' yrbss_cat <- get_catalog( "yrbss" , output_dir = "C:/My Directory/YRBSS" )
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

		unique_directories <- unique( c( catalog$unzip_folder , if( 'output_filename' %in% names( catalog ) ) dirname( catalog$output_filename ) , catalog$dbfolder , catalog$output_folder ) )

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

		catalog <- load_fun( data_name = data_name , catalog , ...)

		cat( paste0( data_name , " local download completed\r\n\n" ) )

		catalog

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
