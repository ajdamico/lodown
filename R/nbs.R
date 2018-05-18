get_catalog_nbs <-
	function( data_name = "nbs" , output_dir , ... ){

		catalog <- NULL
		
		puf_homepage <- "https://www.ssa.gov/disabilityresearch/publicusefiles.html"
	
		nbs_rounds <- gsub( "(.*)nbs_round_(.*)\\.htm(l?)(.*)" , "nbs_round_\\2.htm\\3" , grep( "nbs_round" , readLines( puf_homepage , warn = FALSE ) , value = TRUE ) )
		
		for( this_round in nbs_rounds ){
			
			this_csv_line <- grep( "href(.*)\\.csv" , readLines( paste0( "https://www.ssa.gov/disabilityresearch/" , this_round ) , warn = FALSE ) , ignore.case = TRUE , value = TRUE )[ 1 ]
	
			this_csv <- paste0( "https://www.ssa.gov/disabilityresearch/" , gsub( '(.*)"(.*)"(.*)' , "\\2" , this_csv_line ) )
	
			round_number <- gsub( "nbs_round_(.*)\\.htm" , "\\1" , this_round )
	
			catalog <-
				rbind(
					catalog ,
					data.frame(
						this_round = round_number ,
						full_url = this_csv ,
						output_filename = paste0( output_dir , "/" , "round " , stringr::str_pad( round_number , 2 , pad = "0" ) , ".rds" ) ,
						stringsAsFactors = FALSE
					)
				)
				
		}

		catalog

	}


lodown_nbs <-
	function( data_name = "nbs" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			download.file( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			x <- read.csv( tf , stringsAsFactors = FALSE )
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )
			
			# remove those rounds at the front of the column names
			names( x ) <- gsub( paste0( "^r" , i , "_" ) , "" , names( x ) )
			# that `^` symbol instructs r to only match a pattern at the start of the string.
			
			# add a column of nuthin' but ones.
			x$one <- 1

			catalog[ i , 'case_count' ] <- nrow( x )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

