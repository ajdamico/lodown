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
		
		catalog$dbfolder <- paste0( output_dir , "/MonetDB" )
		
		catalog$db_tablename <- paste0( "pnad" , catalog$year )
		
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

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			# download the sas file
			if( !is.na( catalog[ i , 'sas_ri' ] ) ){
			
				cachaca( catalog[ i , "sas_ri" ] , tf , mode = 'wb' )

				unzipped_files <- c( unzipped_files , unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) ) )

			}






			# manually set the encoding of the unziped files so they don't break things.
			if( catalog[ i , 'year' ] %in% 2013:2014 & .Platform$OS.type != 'windows' ) Encoding( unzipped_files ) <- 'UTF-8' else Encoding( unzipped_files ) <- 'latin1'
			
			
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

			first_attempt_dom <- 
				try({
					# store the PNAD household records as a MonetDBLite database
					read_SAScii_monetdb( 
						dom.fn , 
						dom.sas , 
						zipped = F , 
						tl = TRUE ,
						# this default table naming setup will name the household-level tables dom2001, dom2002, dom2003 and so on
						tablename = paste0( 'dom' , catalog[ i , 'year' ] ) ,
						connection = db
					)

					} , silent = TRUE )
			
			# if the read_SAScii_monetdbattempts broke,
			# remove the dots in the files
			# and try again
			if( class( first_attempt_dom ) == 'try-error' ){
					
				dom.fn2 <- tempfile()
				fpx <- file( normalizePath( dom.fn ) , 'r' , encoding = "windows-1252" )
				# create a write-only file connection to the temporary file
				fpt <- file( dom.fn2 , 'w' )

				# loop through every line in the original file..
				while ( length( line <- readLines( fpx , 1 ) ) > 0 ){
				
					# replace '.' with nothings..
					line <- gsub( " ." , "  " , line , fixed = TRUE )
					line <- gsub( ". " , "  " , line , fixed = TRUE )
					
					# and write the result to the temporary file connection
					writeLines( line , fpt )
				}
				
				# close the temporary file connection
				close( fpx )
				close( fpt )

				# store the PNAD household records as a MonetDBLite database
				read_SAScii_monetdb( 
					dom.fn2 , 
					dom.sas , 
					zipped = F , 
					tl = TRUE ,
					# this default table naming setup will name the household-level tables dom2001, dom2002, dom2003 and so on
					tablename = paste0( 'dom' , catalog[ i , 'year' ] ) ,
					connection = db
				)
				
				unzipped_files <- c( unzipped_files , dom.fn2 )
				
				stopifnot( R.utils::countLines( dom.fn ) == DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM dom" , catalog[ i , 'year' ] ) )[ 1 , 1 ] )
				
			}
			
			first_attempt_pes <- 
				try({
				
					# store the PNAD person records as a MonetDBLite database
					read_SAScii_monetdb( 
						pes.fn , 
						pes.sas , 
						zipped = F , 
						tl = TRUE ,
						# this default table naming setup will name the person-level tables pes2001, pes2002, pes2003 and so on
						tablename = paste0( 'pes' , catalog[ i , 'year' ] ) ,
						connection = db
					)
			
				} , silent = TRUE )
			
			# if the read_SAScii_monetdbattempts broke,
			# remove the dots in the files
			# and try again
			if( class( first_attempt_pes ) == 'try-error' ){

				pes.fn2 <- tempfile()
				
				fpx <- file( normalizePath( pes.fn ) , 'r' , encoding = "windows-1252" )
				# create a write-only file connection to the temporary file
				fpt <- file( pes.fn2 , 'w' )

				# loop through every line in the original file..
				while ( length( line <- readLines( fpx , 1 ) ) > 0 ){
				
					# replace '.' with nothings..
					line <- gsub( " ." , "  " , line , fixed = TRUE )
					line <- gsub( ". " , "  " , line , fixed = TRUE )
					line <- gsub( "\U00A0" , " " , line )

					# and write the result to the temporary file connection
					writeLines( line , fpt )
				}
				
				# close the temporary file connection
				close( fpx )
				close( fpt )
				
			
				# store the PNAD person records as a MonetDBLite database
				read_SAScii_monetdb( 
					pes.fn2 , 
					pes.sas , 
					zipped = F , 
					tl = TRUE ,
					# this default table naming setup will name the person-level tables pes2001, pes2002, pes2003 and so on
					tablename = paste0( 'pes' , catalog[ i , 'year' ] ) ,
					connection = db
				)

				unzipped_files <- c( unzipped_files , pes.fn2 )
				
				stopifnot( R.utils::countLines( pes.fn ) == DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM pes" , catalog[ i , 'year' ] ) )[ 1 , 1 ] )
				
			}
					
			# the ASCII and SAS importation instructions stored in temporary files
			# on the local disk are no longer necessary, so delete them.
			attempt.one <- try( file.remove( unzipped_files ) , silent = TRUE )
			# weird brazilian file encoding operates differently on mac+*nix versus windows, so try both ways.
			if( class( attempt.one ) == 'try-error' ) { Encoding( unzipped_files ) <- '' ; file.remove( unzipped_files ) }
			
			# add 4617 and 4618 to 2001 file
			if( catalog[ i , 'year' ] == 2001 ){
			
				DBI::dbSendQuery( db , "ALTER TABLE dom2001 ADD COLUMN v4617 real" )
				DBI::dbSendQuery( db , "ALTER TABLE dom2001 ADD COLUMN v4618 real" )
			
				DBI::dbSendQuery( db , "UPDATE dom2001 SET v4617 = strat" )
				DBI::dbSendQuery( db , "UPDATE dom2001 SET v4618 = psu" )
				
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
				if( household.nr[ curRow , 'variable' ] %in% DBI::dbListFields( db , paste0( 'dom' , catalog[ i , 'year' ] ) ) ){

					# ..and the variable should be recoded for that year
					if( catalog[ i , 'year' ] %in% eval( parse( text = household.nr[ curRow , 'year' ] ) ) ){
				
						# update all variables where that code equals the `missing` code to NA (NULL in MonetDBLite)
						DBI::dbSendQuery( 
							db , 
							paste0( 
								'update dom' , 
								catalog[ i , 'year' ] , 
								' set ' , 
								household.nr[ curRow , 'variable' ] , 
								" = NULL where " ,
								household.nr[ curRow , 'variable' ] ,
								' = ' ,
								household.nr[ curRow , 'code' ]
							)
						)
					
					}
				}
			}

			# loop through each row in the missing person-level codes table
			for ( curRow in seq( nrow( person.nr ) ) ){

				# if the variable is in the current table..
				if( person.nr[ curRow , 'variable' ] %in% DBI::dbListFields( db , paste0( 'pes' , catalog[ i , 'year' ] ) ) ){
				
					# ..and the variable should be recoded for that year
					if( catalog[ i , 'year' ] %in% eval( parse( text = person.nr[ curRow , 'year' ] ) ) ){
				
						# update all variables where that code equals the `missing` code to NA (NULL in MonetDBLite)
						DBI::dbSendQuery( 
							db , 
							paste0( 
								'update pes' , 
								catalog[ i , 'year' ] , 
								' set ' , 
								person.nr[ curRow , 'variable' ] , 
								" = NULL where " ,
								person.nr[ curRow , 'variable' ] ,
								' = ' ,
								person.nr[ curRow , 'code' ]
							)
						)
					
					}
				}
			}

			# confirm no fields are in `dom` unless they are in `pes`
			b_fields <- DBI::dbListFields( db , paste0( 'dom' , catalog[ i , 'year' ] ) )[ !( DBI::dbListFields( db , paste0( 'dom' , catalog[ i , 'year' ] ) ) %in% DBI::dbListFields( db , paste0( 'pes' , catalog[ i , 'year' ] ) ) ) ]
			
			# create the merged file
			DBI::dbSendQuery( 
				db , 
				paste0( 
					# this default table naming setup will name the final merged tables pes2001, pes2002, pes2003 and so on
					"create table " ,
					catalog[ i , 'db_tablename' ] ,
					# also add a new column "one" that simply contains the number 1 for every record in the data set
					# also add a new column "uf" that contains the state code, since these were thrown out of the SAS script
					# also add a new column "region" that contains the larger region, since these are shown in the tables
					# NOTE: the substr() function luckily works in MonetDBLite::MonetDBLite() databases, but may not work if you change SQL database engines to something else.
					" as select a.* , " ,
					paste( b_fields , collapse = "," ) ,
					" , 1 as one , substr( a.v0102 , 1 , 2 ) as uf , substr( a.v0102 , 1 , 1 ) as region from pes" , 
					catalog[ i , 'year' ] , 
					" as a inner join dom" , 
					catalog[ i , 'year' ] , 
					" as b on a.v0101 = b.v0101 AND a.v0102 = b.v0102 AND a.v0103 = b.v0103" 
				)
			)

			# determine if the table contains a `v4619` variable.
			# v4619 is the factor of subsampling used to compensate the loss of units in some states
			# for 2012, the variable v4619 is one and so it is not needed.
			# if it does not, create it.
			any.v4619 <- 'v4619' %in% DBI::dbListFields( db , catalog[ i , 'db_tablename' ] )

			# if it's not in there, copy it over
			if ( !any.v4619 ) {
				DBI::dbSendQuery( db , paste0( 'alter table ' , catalog[ i , 'db_tablename' ] , ' add column v4619 real' ) )
				DBI::dbSendQuery( db , paste0( 'update ' , catalog[ i , 'db_tablename' ] , ' set v4619 = 1' ) )
			}
			
			# now create the pre-stratified weight to be used in all of the survey designs
			# if it's not in there, copy it over
			DBI::dbSendQuery( db , paste0( 'alter table ' , catalog[ i , 'db_tablename' ] , ' add column pre_wgt real' ) )

			if( catalog[ i , 'year' ] < 2004 ){
				DBI::dbSendQuery( db , paste0( 'update ' , catalog[ i , 'db_tablename' ] , ' set pre_wgt = v4610' ) )
			} else {
				DBI::dbSendQuery( db , paste0( 'update ' , catalog[ i , 'db_tablename' ] , ' set pre_wgt = v4619 * v4610' ) )	
			}
			
			# confirm that the number of records in the pnad merged file
			# matches the number of records in the person file
			stopifnot( 
				DBI::dbGetQuery( db , paste0( "select count(*) as count from pes" , catalog[ i , 'year' ] ) ) == 
				DBI::dbGetQuery( db , paste0( "select count(*) as count from " , catalog[ i , 'db_tablename' ] ) ) 
			)


			catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , catalog[ i , 'db_tablename' ] ) )


			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored as '" , catalog[ i , 'db_tablename' ] , "' in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

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
		design$postStrata <- list(index)

		# return the updated database-backed survey design object
		design
	}

# thanks for playing
