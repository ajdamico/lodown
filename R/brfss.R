get_catalog_brfss <-
	function( data_name = "brfss" , output_dir , ... ){
			
		data_page <- readLines( "https://www.cdc.gov/brfss/annual_data/annual_data.htm" )

		available_years <- sort( unique( gsub( "(.*)/brfss/annual_data/annual_([0-9][0-9][0-9][0-9]).htm(.*)" , "\\2" , grep( "annual_data/annual_([0-9][0-9][0-9][0-9]).htm" , data_page , value = TRUE ) ) ) )

		path_to_files <-
			ifelse( available_years < 1990 , 
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/CDBRFS" , substr( available_years , 3 , 4 ) , "_XPT.zip" ) ,
			ifelse( available_years < 2002 , 
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/CDBRFS" , substr( available_years , 3 , 4 ) , "XPT.zip" ) ,
			ifelse( available_years >= 2012 ,
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/LLCP" , available_years , "ASC.ZIP" ) ,
			ifelse( available_years == 2011 ,
				"https://www.cdc.gov/brfss/annual_data/2011/files/LLCP2011ASC.ZIP" ,
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/CDBRFS" , ifelse( available_years == 2002 , available_years , substr( available_years , 3 , 4 ) ) , "ASC.zip" )
				) ) ) )

		sas_files <-
			ifelse( available_years < 2002 ,
				NA ,
			ifelse( available_years >= 2012 ,
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/sasout" , substr( available_years , 3 , 4 ) , "_llcp.sas" ) ,
			ifelse( available_years == 2011 ,
				"https://www.cdc.gov/brfss/annual_data/2011/sasout11_llcp.sas" ,
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/sasout" , substr( available_years , 3 , 4 ) , ifelse( available_years > 2006 , ".SAS" , ".sas" ) ) 
				) ) )


		catalog <-
			data.frame(
				year = available_years ,
				full_url = path_to_files ,
				sas_ri = sas_files ,
				output_filename = paste0( output_dir , "/" , available_years , " main.rds" ) ,
				
				# design information
				weight = c( rep( 'x_finalwt' , 18 ) , rep( 'xfinalwt' , 9 ) , rep( 'xllcpwt' , length( available_years ) - 27 ) ) ,
				psu = c( rep( 'x_psu' , 18 ) , rep( 'xpsu' , length( available_years ) - 18 ) ) ,
				strata = c( rep( 'x_ststr' , 18 ) , rep( 'xststr' , length( available_years ) - 18 ) ) ,

				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_brfss <-
	function( data_name = "brfss" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile() ; impfile <- tempfile() ; sasfile <- tempfile() ; csvfile <- tempfile()

		
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , filesize_fun = 'unzip_verify' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			if( is.na( catalog[ i , 'sas_ri' ] ) ){
			
				# read the sas transport file into r
				x <- foreign::read.xport( unzipped_files ) 
				
			} else {
			
				sas_con <- file( catalog[ i , 'sas_ri' ] , "rb" , encoding = "windows-1252" )
				z <- readLines( sas_con , encoding = 'latin1' )
				close( sas_con )
						
				# throw out a few columns that cause importation trouble with monetdb
				if ( catalog[ i , 'year' ] == 2009 ) z <- z[ -159:-168 ]
				if ( catalog[ i , 'year' ] == 2011 )	z <- z[ !grepl( "CHILDAGE" , z ) ]
				if ( catalog[ i , 'year' ] == 2013 ) z[ 361:362 ] <- c( "_FRTLT1z       2259" , "_VEGLT1z       2260" )
				if ( catalog[ i , 'year' ] == 2014 ) z[ 86 ] <- "COLGHOUS $ 64"

				if( catalog[ i , 'year' ] == 2015 ){
				
					z <- gsub( "\\\f" , "" , z )
					z <- gsub( "_FRTLT1       2056" , "_FRTLT1_       2056" , z )
					z <- gsub( "_VEGLT1       2057" , "_VEGLT1_       2057" , z )
					
				}
				
				# replace all underscores in variable names with x's
				z <- gsub( "_" , "x" , z , fixed = TRUE )
				
				# throw out these three fields, which overlap other fields and therefore are not supported by SAScii
				# (see the details section at the bottom of page 9 of http://cran.r-project.org/web/packages/SAScii/SAScii.pdf for more detail)
				z <- z[ !grepl( "SEQNO" , z ) ]
				z <- z[ !grepl( "IDATE" , z ) ]
				z <- z[ !grepl( "PHONENUM" , z ) ]
				
				# remove all special characters
				z <- gsub( "\t" , " " , z , fixed = TRUE )
				z <- gsub( "\f" , " " , z , fixed = TRUE )
				
				# re-write the sas importation script to a file on the local hard drive
				writeLines( z , impfile )
	
				x <- 
					read_SAScii (
						unzipped_files ,
						impfile ,
						beginline = 70
					)
					
			}

			# convert all column names in the table to all lowercase
			names( x ) <- tolower( names( x ) )
			
			x$one <- 1
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			# add the number of records to the catalog
			catalog[ i , 'case_count' ] <- nrow( x )

			# delete the temporary files
			suppressWarnings( file.remove( tf , impfile , unzipped_files , sasfile , csvfile ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

