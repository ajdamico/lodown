get_catalog_yrbss <-
	function( data_name = "yrbss" , output_dir , ... ){

		catalog <- NULL
	
		yrbss_ftp_contents <- strsplit( RCurl::getURL( "https://ftp.cdc.gov/pub/data/yrbs/" , ssl.verifypeer = FALSE ) , "<br>" )[[1]]

		yrbss_ftp_paths <- paste0( "https://ftp.cdc.gov/pub/data/yrbs/" , gsub( '(.*)\\">(.*)<\\/A>$', "\\2" , yrbss_ftp_contents ) , "/" )

		yrbss_folders <- grep( "([0-9][0-9][0-9][0-9])" , yrbss_ftp_paths , value = TRUE )
		
		yrbss_folders <- yrbss_folders[ !( basename( yrbss_folders ) %in% c( "2015" , "SADC_2013" ) ) ]
		
		for( this_year in yrbss_folders ){
				
			this_year_contents <- strsplit( RCurl::getURL( this_year , ssl.verifypeer = FALSE ) , "<br>" )[[1]]

			this_year_paths <- paste0( this_year , gsub( '(.*)\\">(.*)<\\/A>$', "\\2" , this_year_contents ) )

			catalog <-
				rbind(
					catalog ,
					data.frame(
						directory = gsub( "(.*)([0-9][0-9][0-9][0-9])(.*)" , "\\2" , basename( this_year ) ) ,
						year = gsub( "(.*)([0-9][0-9][0-9][0-9])(.*)" , "\\2" , basename( this_year ) ) ,
						dat_url = grep( "\\.dat$" , this_year_paths , value = TRUE , ignore.case = TRUE ) ,
						sas_url = grep( "input_program\\.sas$" , this_year_paths , value = TRUE , ignore.case = TRUE ) ,
						stringsAsFactors = FALSE
					)
			)
		}
		
		# hardcode 2015 since it doesn't fit the pattern
		catalog[ catalog[ , 'year' ] == 2015 , 'dat_url' ] <- "https://www.cdc.gov/healthyyouth/data/yrbs/files/yrbs2015.dat"
		
		catalog$output_filename <- paste0( output_dir , "/" , catalog$year , " main.rds" )
			
		catalog

	}


lodown_yrbss <-
	function( data_name = "yrbss" , catalog , ... ){

		on.exit( print( catalog ) )

		tf_fn <- tempfile()
		tf_sas <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "dat_url" ] , tf_fn , mode = 'wb' )
			
			# read those sas importation instructions
			# into working memory immediately
			sas_text <- tolower( readLines( catalog[ i , "sas_url" ] ) )

			# if there's an @1 site $3. out of order,
			# re-order the text file so it goes in the front
			if( any( sas_text == '@1 site $3.' ) ){
			
				# find the site location
				site.location <- which( sas_text == '@1 site $3.' )
				
				# find the start field location
				input.location <- which( sas_text == "input" )
				
				# create a vector from 1 to the length of the text file
				sas_length <- seq( length( sas_text ) )
				
				# remove the site location
				sas_length <- sas_length[ -site.location ]
				
				# re-insert the site location right after input
				sas_reorder <- c( sas_length[ seq( input.location ) ] , site.location , sas_length[ seq( input.location + 1 , length( sas_length ) ) ] )

				# re-order the sas text file
				sas_text <- sas_text[ sas_reorder ]
			}
			
			
			# older sas import scripts have some
			# serious quirks that need to be twerked out.
			if ( catalog[ i , "year" ] < 2013 ){
					
				# the R SAScii package cannot handle `$char8.`
				# so here's the first half of q4 patch,
				# where those lines of the sas importation scripts
				# are manually dealt with
				sas_text <- gsub( "q4orig $char8." , "q4orig  8.0" , sas_text , fixed = TRUE )
				
				# find all strings that begin with "@"
				at.beginners <- which( substr( sas_text , 1 , 1 ) == "@" )
				
				# remove all "" empty strings
				no.empty <- lapply( strsplit( sas_text[ at.beginners ] , " " ) , function( z ) z[ z != '' ] )
				
				# take all of the _second_ elements
				vars.to.flip <- lapply( no.empty , "[[" , 2 )
				
				# repeatedly run the previously-constructed `sas.switcharoo` function
				# on all of the lines that need lines flipped
				for ( var.to.flip in vars.to.flip ) sas_text <- sas.switcharoo( sas_text , var.to.flip )	
				
				# and here's the second half of q4 patch
				# q4orig should be treated as a string, not numeric
				sas_text <- gsub( "q4orig" , "q4orig $" , sas_text , fixed = TRUE )

				# here's some more code to deal with quirky sas importation instructions:
				
				# if the first column position isn't at one..
				first.instruction <- grep( 'input' , SAScii::SAS.uncomment( sas_text , '/*' , '*/') ) + 1
				
				if ( !grepl( " 1-" , sas_text[ first.instruction ] ) ){
				
					# find the first position
					dash.position <- gregexpr( "-" , sas_text[ first.instruction ] )[[1]][1]
					start.blank <- as.numeric( substr( sas_text[ first.instruction ] , dash.position - 3 , dash.position - 1 ) ) - 1
					
					# add a blank in sas_text
					sas_text <-
						c(
							sas_text[ 1:( first.instruction - 1 ) ] ,
							paste0( "blank $ 1-" , start.blank ) ,
							sas_text[ first.instruction:length( sas_text ) ]
						)	
					# adding this `blank` will effectively create a column full of nothing
					# in the final data file as read-in by read.SAScii
					# ..but that'll get thrown out later
				}

			}
				
				
			# save the final sas importation script to
			# a file on the hard disk (since ?read.SAScii and ?parse.SAScii
			# require a file, not something read into working memory)
			writeLines( sas_text , tf_sas )
			
			x <- read_SAScii( tf_fn , tf_sas )
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			# throw out all columns called `blank`
			# which we'd added as a band-aid above.
			x <- x[ , !( names( x ) %in% 'blank' ) ]
				
			# add a column full of ones
			x$one <- 1
			
			catalog[ i , 'case_count' ] <- nrow( x )
			
			# save the current `x` data.frame to the local disk
			saveRDS( x , file = catalog[ i , "output_filename" ] , compress = FALSE )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}
		
		# delete the temporary files
		file.remove( tf_fn , tf_sas )

		on.exit()
		
		catalog

	}


sas.switcharoo <-
	function( sas_ri , variable ){
			
		# find the first occurrence of the variable inside the sas importation syntax
		ows.fo <- grep( variable , sas_ri )[1]
		
		# extract that single line of code
		old.ws <- sas_ri[ ows.fo ]
		
		# find the ending position of the variable within the current string
		end.position.of.var <- eval( parse( text = gsub( paste( "@(.*)" , variable , "(.*)\\.(.*)" ) , "\\1 + \\2" , old.ws ) ) ) - 1
		
		# find the replacement string
		new.ws <- gsub( paste( "@(.*)" , variable , "(.*)\\." ) , paste0( variable , " \\1-" , end.position.of.var , " ." ) , old.ws , perl = TRUE )
		
		# replace the old block with the new one
		# throughout the sas importation instructions
		gsub( old.ws , new.ws , sas_ri )
		# since the result of this `gsub` function
		# is the last line of the `sas.switcharoo` function
		# the function will return that result.
	}
	