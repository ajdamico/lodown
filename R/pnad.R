get_catalog_pnad <-
	function( data_name = "pnad" , output_dir , ... ){

		# initiate the full ftp path
		year.ftp <- "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_anual/microdados/"

		# read the text of the microdata ftp into working memory
		# download the contents of the ftp directory for all microdata
		year.listing <- readLines( textConnection( RCurl::getURL( year.ftp ) ) )

		# extract all years
		year.lines <- gsub( "(.*)([0-9][0-9][0-9][0-9])" , "\\2" , year.listing )
		
		# available years 2013+
		suppressWarnings( year.lines <- year.lines[ !is.na( as.numeric( year.lines ) ) & year.lines >= 2013 ] )

		catalog <-
			data.frame(
				year = c( 2001:2009 , 2011:2012 , year.lines ) ,
				ftp_folder = paste0( year.ftp , c( rep( 'reponderacao_2001_2012' , 11 ) , year.lines ) , '/' ) ,
				stringsAsFactors = FALSE
			)
		
		
		# loop through every year
		for ( this_entry in seq( nrow( catalog ) ) ){

			# find the zipped files in the year-specific folder
			ftp.listing <- readLines( textConnection( RCurl::getURL( catalog[ this_entry , 'ftp_folder' ] ) ) )
			
			filenames <- gsub( "(.*) (.*)" , "\\2" , ftp.listing ) ; filenames <- filenames[ filenames != "" ]
							
			if( catalog[ this_entry , 'year' ] >= 2013 ){

				catalog[ this_entry , 'full_url' ] <- paste0( catalog[ this_entry , 'ftp_folder' ] , grep( "^dados" , filenames , ignore.case = TRUE , value = TRUE ) )

				catalog[ this_entry , 'sas_ri' ] <- paste0( catalog[ this_entry , 'ftp_folder' ] , grep( "^dicionarios" , filenames , ignore.case = TRUE , value = TRUE ) )
			
			} else {
			
				catalog[ this_entry , 'full_url' ] <- paste0( catalog[ this_entry , 'ftp_folder' ] , grep( paste0( "pnad_reponderado_" , catalog[ this_entry , 'year' ] ) , filenames , ignore.case = TRUE , value = TRUE ) )

			}
			
		}
		
		catalog$output_filename <- paste0( output_dir , "/" , catalog$year , " main.rds" )
		
		catalog

	}


lodown_pnad <-
	function( data_name = "pnad" , catalog , ... ){

		tf <- tempfile()

		
				
		# download and import the tables containing missing codes
		household.nr <- read.csv( system.file("extdata", "pnad/household_nr.csv", package = "lodown") , colClasses = 'character' )

		person.nr <- read.csv( system.file("extdata", "pnad/person_nr.csv", package = "lodown") , colClasses = 'character' )

		# convert these tables to lowercase
		names( household.nr ) <- tolower( names( household.nr ) )
		names( person.nr ) <- tolower( names( person.nr ) )

		# remove all spaces between missing codes
		household.nr$code <- gsub( " " , "" , household.nr$code )
		person.nr$code <- gsub( " " , "" , person.nr$code )

		# convert all code column names to lowercase
		household.nr$variable <- tolower( household.nr$variable )
		person.nr$variable <- tolower( person.nr$variable )

		
		
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			# download the sas file
			if( !is.na( catalog[ i , 'sas_ri' ] ) ){
			
				cachaca( catalog[ i , "sas_ri" ] , tf , mode = 'wb' )

				unzipped_files <- c( unzipped_files , unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) ) )

			}






			# manually set the encoding of the unziped files so they don't break things.
			if( catalog[ i , 'year' ] > 2012 & .Platform$OS.type != 'windows' ) Encoding( unzipped_files ) <- 'UTF-8' else Encoding( unzipped_files ) <- 'latin1'
			
			
			# remove the UF column and the mistake with "LOCAL ULTIMO FURTO"
			# described in the pnad_remove_uf() function that was loaded with source_url as pnad.survey.R
			dom.sas <- pnad_remove_uf( unzipped_files[ grepl( paste0( 'input[^?]dom' , catalog[ i , 'year' ] , '.txt' ) , tolower( unzipped_files ) ) ] )
			pes.sas <- pnad_remove_uf( unzipped_files[ grepl( paste0( 'input[^?]pes' , catalog[ i , 'year' ] , '.txt' ) , tolower( unzipped_files ) ) ] )

			# in 2003 and 2007, the age variable had been read in as a factor variable
			# which breaks certain commands by treating the variable incorrectly as a factor
			if( catalog[ i , 'year' ] %in% c( 2003 , 2007 ) ){
				pes_con <- file( pes.sas , "r" , encoding = "windows-1252" )
				pes_lines <- readLines( pes_con )
				close( pes_con )
				pes_lines <- iconv( pes_lines , "" , "ASCII//TRANSLIT" )
				pes_lines <- gsub( "@00027( *)V8005( *)\\$3\\." , "@00027 V8005 3\\." , pes_lines )
				writeLines( pes_lines , pes.sas )
			}
			
			# since `files` contains multiple file paths,
			# determine the filepath on the local disk to the household (dom) and person (pes) files
			dom.fn <- unzipped_files[ grepl( paste0( '/dom' , catalog[ i , 'year' ] ) , tolower( unzipped_files ) ) ]
			pes.fn <- unzipped_files[ grepl( paste0( '/pes' , catalog[ i , 'year' ] ) , tolower( unzipped_files ) ) ]

			dom_df <-
				read_SAScii(
					dom.fn , 
					dom.sas ,
					zipped = FALSE ,
					na = c( "" , "NA" , "." ) ,
					guess_max = 100000
				)
			
			pes_df <-
				read_SAScii(
					pes.fn ,
					pes.sas ,
					zipped = FALSE ,
					na = c( "" , "NA" , "." ) ,
					guess_max = 100000
				)
	
			names( dom_df ) <- tolower( names( dom_df ) )
			names( pes_df ) <- tolower( names( pes_df ) )
	
			# the ASCII and SAS importation instructions stored in temporary files
			# on the local disk are no longer necessary, so delete them.
			attempt.one <- try( file.remove( unzipped_files ) , silent = TRUE )
			# weird brazilian file encoding operates differently on mac+*nix versus windows, so try both ways.
			if( class( attempt.one ) == 'try-error' ) { Encoding( unzipped_files ) <- '' ; file.remove( unzipped_files ) }
			
			# add 4617 and 4618 to 2001 file
			if( catalog[ i , 'year' ] == 2001 ){
			
				dom_df$v4617 <- dom_df$strat
				dom_df$v4618 <- dom_df$psu
				
			}
			
			# missing level blank-outs #
			# this section loops through the non-response values & variables for all years
			# and sets those variables to NULL.
			# cat( 'non-response variable blanking-out only occurs on numeric variables\n' )
			# cat( 'categorical variable blanks are usually 9 in the pnad\n' )
			# cat( 'thanks for listening\n' )
			
			# loop through each row in the missing household-level  codes table
			for ( curRow in seq( nrow( household.nr ) ) ){

				# if the variable is in the current table..
				if( household.nr[ curRow , 'variable' ] %in% names( dom_df ) ){

					# ..and the variable should be recoded for that year
					if( catalog[ i , 'year' ] %in% eval( parse( text = household.nr[ curRow , 'year' ] ) ) ){
				
						dom_df[ dom_df[ , household.nr[ curRow , 'variable' ] ] %in% household.nr[ curRow , 'code' ] , household.nr[ curRow , 'variable' ] ] <- NA
					
					}
				}
			}

			# loop through each row in the missing person-level codes table
			for ( curRow in seq( nrow( person.nr ) ) ){

				# if the variable is in the current table..
				if( person.nr[ curRow , 'variable' ] %in% names( pes_df ) ){
				
					# ..and the variable should be recoded for that year
					if( catalog[ i , 'year' ] %in% eval( parse( text = person.nr[ curRow , 'year' ] ) ) ){
				
				
						pes_df[ pes_df[ , person.nr[ curRow , 'variable' ] ] %in% person.nr[ curRow , 'code' ] , person.nr[ curRow , 'variable' ] ] <- NA
					
					
					}
				}
			}

			# confirm no fields are in `dom` unless they are in `pes`
			b_fields <- c( 'v0101' , 'v0102' , 'v0103' , setdiff( names( pes_df ) , names( dom_df ) ) )
			
			pes_df <- pes_df[ b_fields ]
			
			pes_df$uf <- substr( pes_df$v0102 , 1 , 2 )
			
			pes_df$region <- substr( pes_df$v0102 , 1 , 1 )
			
			pes_df$one <- 1
			
			x <- merge( dom_df , pes_df )
			
			stopifnot( nrow( x ) == nrow( pes_df ) ) ; rm( pes_df , dom_df ) ; gc()
			
			# determine if the table contains a `v4619` variable.
			# v4619 is the factor of subsampling used to compensate the loss of units in some states
			# for 2012, the variable v4619 is one and so it is not needed.
			# if it does not, create it.
			if( !( 'v4619' %in% names( x ) ) ) x$v4619 <- 1

			
			if( catalog[ i , 'year' ] < 2004 ){
				x$pre_wgt <- x$v4610
			} else {
				x$pre_wgt <- x$v4619 * x$v4610
			}
			

			catalog[ i , 'case_count' ] <- nrow( x )

			saveRDS( x , file = catalog[ i , 'output_filename' ] ) ; rm( x ) ; gc()
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		catalog

	}


	
# initiate a function that removes the "UF" field from the PNAD SAS importation script,
# because the R `SAScii` package does not currently handle overlapping fields and this UF column always overlaps
pnad_remove_uf <-
	function( sasfile ){

		# read the SAS import file into R
		sascon <- file( sasfile , "r" , blocking = FALSE , encoding = "windows-1252" )
		sas_lines <- readLines( sascon )
		close( sascon )
		
		sas_lines <- iconv( sas_lines , "" , "ASCII//TRANSLIT" )
		
		# remove any TAB characters, replace them with two spaces
		sas_lines <- gsub( "\t" , "  " , sas_lines )
		
		# throw out any lines that contain the UF line
		sas_lines <- sas_lines[ !grepl( "@00005[ ]+UF[ ]" , sas_lines ) ]

		# fix the "$1 ." difference seen in a number of PNAD SAS importation scripts
		# by simply switching the space and the period
		sas_lines <- 
			gsub(
				"@00840  V2913  $1 ." ,
				"@00840  V2913  $1. " ,
				sas_lines ,
				fixed = TRUE
			)
		
		# fix the duplicate column names in 2007
		sas_lines <-
			gsub(
				"00965  V9993" ,
				"00965  V9993A" ,
				sas_lines ,
			)
		
		# create a temporary file
		tf <- tempfile()

		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
}


# initiate a function that post-stratifies the PNAD survey object,
# because the R `survey` package does not currently allow post-stratification of database-backed survey objects
pnad_postStratify <-
	function( design , strata.col , oldwgt ){
		
		# extract the tablename within the MonetDBLite database
		tablename <- design$db$tablename
		
		# extract the MonetDBLite connection
		conn <- design$db$connection

		# create an R data frame containing one record per strata
		# that will be used to determine the weights-multiplier for each strata in survey dataset
		# this table contains one record per strata
		population <- 
			DBI::dbGetQuery( 
				conn , 
				paste(
					'select' ,
					strata.col ,
					", CAST( " ,
					strata.col ,
					' AS DOUBLE ) as newwgt , sum( ' ,
					oldwgt ,
					' ) as oldwgt from ' ,
					tablename , 
					'group by' ,
					strata.col
				)
			)
		
		# calculate the multiplier
		population$mult <- population$oldwgt / population$newwgt
		
		# retain only the strata identifier and the multiplication value
		population <- population[ , c( strata.col , 'mult' ) ]
		
		# pull the strata and the original weight variable from the original table
		# this data.frame contains one record per respondent in the PNAD dataset
		# as opposed to one record per strata
		so.df <- 
			DBI::dbGetQuery( 
				conn , 
				paste( 
					'select' , 
					strata.col , 
					"," ,
					oldwgt ,
					'from' , 
					tablename 
				) 
			)
		
		# add a row number variable to re-order post-merge
		so.df$n <- 1:nrow( so.df )
			
		# merge the strata with the multipliers
		so.df <-
			merge( so.df , population )
		
		# since ?merge undid the order relative to the original table,
		# put the strata and old weight table back in order
		so.df <-
			so.df[ order( so.df$n ) , ]
		
		# extract the multipliers into a numeric vector
		prob.multipliers <- so.df[ , 'mult' ]
		
		# overwrite the design's probability attribute with post-stratified probabilities
		design$prob <- design$prob * prob.multipliers

		# construct the `postStrata` attribute of the survey design object
		index <- as.numeric( so.df[ , strata.col ] )
		
		# extract the original weights..
		attr( index , 'oldweights' ) <- so.df[ , oldwgt ]
		
		# ..and the new weights
		attr( index , 'weights' ) <-  1 / design$prob
		
		# so that the standard errors accurately reflect the
		# process of post-stratification
		design$postStrata <- c( design$postStrata  , list(index) )

		# return the updated database-backed survey design object
		design
	}

# thanks for playing
