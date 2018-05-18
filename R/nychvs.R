get_catalog_nychvs <-
	function( data_name = "nychvs" , output_dir , ... ){

		catalog <- NULL

		# hardcoded catalog because nychvs will be incorporated into ahs going forward
		for( year in c( 2002 , 2005 , 2008 , 2011 , 2014 ) ){
		
			# create three year-specific variables:
			
			# the last two digits of the current year
			subyear <- substr( year , 3 , 4 )
			
			# they started naming things differently in 2011
			if( year >= 2011 ) {
				filetypes <- c( 'occ' , 'vac' , 'pers' ) 
			} else {
				filetypes <- c( 'occ' , 'vac' , 'per' , 'ni' )
			}
			
			prefix <- ifelse( year > 2008 , paste0( "/uf_" , subyear ) , "/lng08" )
			
			# loop through each available filetype
			for ( filetype in filetypes ){

				# construct the url of the file to download #

				census_url <-
					paste0( 
						"https://www2.census.gov/programs-surveys/nychvs/datasets/" , 
						year , 
						"/microdata" ,
						prefix , 
						"_" , 
						filetype , 
						ifelse( year < 2011 , subyear , '' ) , 
						ifelse( 
							year < 2014 &
							filetype %in% c( 'occ' , 'pers' , 'per' ) , 
							'_rev' , 
							'_web' 
						) , 
						ifelse( year == 2014 & filetype != 'vac' , "_b" , "" ) ,
						ifelse( 
							( year == 2011 & filetype == 'vac' ) | ( year == 2014 & filetype != 'vac' ) , 
							".txt" , 
							".dat" 
						)
					)

				# the `census.url` object now contains the complete filepath
					
				# construct the url of the SAS importation script #
				
				if( year < 2014 ){
				
					# massive thanx to http://furmancenter.org for providing these.
					sas_script <- system.file("extdata", paste( "nychvs/furman/hvs" , subyear , filetype , "load.sas" , sep = "_" ) , package = "lodown")
					
					beginline <- 1
					
				} else {

					# set the import script begin lines.
					if( filetype == 'occ' ) {
						beginline <- 9 
					} else if ( filetype == 'vac' ) {
						beginline <- 561 
					} else if ( filetype == 'pers' ){
						beginline <- 413
					} else stop( "this filetype hasn't been implemented yet." )
					
					sas_script <- paste0( "https://www2.census.gov/programs-surveys/nychvs/datasets/" , year , "/microdata/sas_import_program.txt" )
					
				}

	
			
			
				this_catalog <-
					data.frame(
						type = filetype ,
						year = year ,
						full_url = census_url ,
						sas_ri = sas_script ,
						beginline = beginline ,
						output_filename = paste0( output_dir , "/" , year , "/" , filetype , ".rds" ) ,
						stringsAsFactors = FALSE
					)

				catalog <- rbind( catalog , this_catalog )
			
			}
		
		}
	
	
		catalog

	}


lodown_nychvs <-
	function( data_name = "nychvs" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# ..and clean it up using the function defined above
			cleaned.sas.script <- nychvs_sas_cleanup( catalog[ i , "sas_ri" ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			# read the file into a data frame
			x <- read_SAScii( tf , cleaned.sas.script , beginline = catalog[ i , 'beginline' ] )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			# add a column of all ones
			x$one <- 1
			
			# household weights need to be divided by one hundred thousand,
			# person-weights need to be divided by ten for more recent years
			# but starting in 2014, this was no longer a problem.
			if ( catalog[ i , 'year' ] < 2014 ){
				if ( !( catalog[ i , 'type' ] %in% c( 'per' , 'pers' ) ) ) {
					x$hhweight <- x$hhweight / 10^5 
				} else if ( catalog[ i , 'year' ] > 2005 ) x$perwgt <- x$perwgt / 10
			}

			catalog[ i , 'case_count' ] <- nrow( x )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}



# initiate a function that will clean up doubly-embedded /* */ - which are allowed in SAS but not SAScii
# remove double embedded /* */ in the code, which the SAScii package does not like
nychvs_sas_cleanup <-
	function( z ) {
	
		# create a temporary file on the local disk
		cleaned.sas.input.script <- tempfile()

		# read the script into memory
		y <- readLines( z )

		# fix typo in 2014 sas program
		y <- gsub( "@514            uf39a           $7." , "@514            uf39a           $1." , y , fixed = TRUE )
		
		# also, while we're removing stuff we don't like, throw out `TAB` characters
		z <- gsub( "\t" , " " , SAScii::SAS.uncomment( SAScii::SAS.uncomment( y , "/*" , "*/" ) , "/*" , "*/" ) )
		
		# get rid of this crap
		z <- gsub( "comma([0-9])\\.([0-9])" , "\\1.\\2" , z )

		# re-write the furman SAS file into an uncommented SAS script
		writeLines( z , cleaned.sas.input.script )
	
		# return the filepath of the saved script on the local disk
		cleaned.sas.input.script
	}
	
