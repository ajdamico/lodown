get_catalog_ess <-
  function( data_name = "ess" , output_dir , ... ){

  	catalog <- NULL

	# figure out all integrated file locations
	ifl <- httr::GET( "http://www.europeansocialsurvey.org/data/round-index.html" )

	# extract all integrated filepaths
	ifl.block <- XML::htmlParse( ifl , asText = TRUE )

	# find all links, with parsing explanation thanks to
	# http://stackoverflow.com/questions/19097089/how-to-extract-childrenhtml-contents-from-an-xmldocumentcontent-object
	z <- XML::xpathSApply( ifl.block , "//a", function(u) XML::xmlAttrs(u)["href"])

	# isolate all links to unduplicated file downloads
	downloads <- unique( z[ grep( "download.html?file=" , z , fixed = TRUE ) ] )

	for( this_download in downloads ){

		# download the integrated file's whole page
		download.page <- httr::GET( paste0( "http://www.europeansocialsurvey.org" , this_download ) )

		# again, convert the page to an R-readable format..
		download.block <- XML::htmlParse( download.page , asText = TRUE )

		# ..then extract all of the links.
		z <- XML::xpathSApply( download.block , "//a", function(u) XML::xmlAttrs(u)["href"])

		# extract the only link with the text `spss` in it.
		spss.file <- z[ grep( "spss" , z ) ]
		
		catalog <-
			rbind(
				catalog ,
				data.frame(
					directory = "integrated" ,
					wave = gsub( "(.*)ESS([0-9]+)e(.*)" , "\\2" , this_download ) ,
					full_url = spss.file ,
					stringsAsFactors = FALSE
				)
			)
			
	}
			
	for( current_round in unique( catalog$wave ) ){

	    cat( paste0( "building " , data_name , " catalog for wave " , current_round , "\r\n\n" ) )

		download_page <- httr::GET( paste0( "http://www.europeansocialsurvey.org/data/download.html?r=" , current_round ) )

		# again, convert the page to an R-readable format..
		download_block <- XML::htmlParse( download_page , asText = TRUE )

		# ..then extract all of the links.
		z <- XML::xpathSApply( download_block , "//a", function(u) XML::xmlAttrs(u)["href"])

		# remove all e-mail addresses
		z <- z[ !grepl( 'mailto' , z ) ]

		# remove all files ending in `.html`
		z <- z[ tools::file_ext( z ) != 'html' ]

		# isolate the filepaths of all pdf files
		pdfs <- z[ tools::file_ext( z ) == 'pdf' ]

		catalog <-
			rbind(
				catalog ,
				data.frame(
					directory = "docs" ,
					wave = current_round ,
					full_url = pdfs ,
					stringsAsFactors = FALSE
				)
			)

		# isolate the filepaths of all current round file downloads
		crd <- unique( z[ grep( paste0( "/download.html?file=ESS" , current_round ) , z , fixed = TRUE ) ] )
		
		for( this_download in crd ){

			# download the integrated file's whole page
			download.page <- httr::GET( paste0( "http://www.europeansocialsurvey.org" , this_download ) )

			# again, convert the page to an R-readable format..
			download.block <- XML::htmlParse( download.page , asText = TRUE )

			# ..then extract all of the links.
			z <- XML::xpathSApply( download.block , "//a", function(u) XML::xmlAttrs(u)["href"])

			# extract the only link with the text `spss` in it.
			spss.file <- z[ grep( "spss" , z ) ]
			
			catalog <-
				rbind(
					catalog ,
					data.frame(
						directory = "countries" ,
						wave = current_round ,
						full_url = spss.file ,
						stringsAsFactors = FALSE
					)
				)

		}
			
	}

	catalog$file_name <- gsub( "(.*)f=(.*)&c(.*)" , "\\2" , catalog$full_url )

	catalog$file_name <- basename( gsub( "(.*)f=(.*)&y(.*)" , "\\2" , catalog$file_name ) )
		
	catalog$year <- as.numeric( catalog$wave ) * 2 + 2000
	
	catalog$output_filename <- 
		ifelse( 
			catalog$directory == 'docs' ,
			paste0( output_dir , "/" , catalog$year , "/docs/" , catalog$file_name ) ,
			paste0( output_dir , "/" , catalog$year , "/" , gsub( "\\.(.*)" , "" , catalog$file_name ) , ".rds" )
		)
	
	catalog$full_url <- paste0( "http://www.europeansocialsurvey.org" , catalog$full_url )
	
	no_country_allowed <- catalog[ catalog$directory == 'integrated' , 'full_url' ]
	
	catalog <- catalog[ !( catalog$directory == 'countries' & catalog$full_url %in% no_country_allowed ) , ]
	
	# skip ESS7 - parents' occupation..all country files and integrated isco file
	catalog <- catalog[ !grepl( "PoccInte" , catalog[ , 'file_name' ] ) , ]
	
	# skip ESS1 - contacts file
	catalog <- catalog[ !grepl( "ESS1CFe01" , catalog[ , 'file_name' ] ) , ]
	
	
	catalog
  
  }


lodown_ess <-
  function( data_name = "ess" , catalog , ... ){

	on.exit( print( catalog ) )

	if ( !requireNamespace( "memisc" , quietly = TRUE ) ) stop( "memisc needed for this function to work. to install it, type `install.packages( 'memisc' )`" , call. = FALSE )

	if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at http://www.europeansocialsurvey.org/user/new" )
	
	your_email <- list(...)[["your_email"]]
	
	tf <- tempfile()

	# store your e-mail address in a list to be passed to the website
	values <- list( u = your_email )

	# authenticate on the ess website
	httr::POST( "http://www.europeansocialsurvey.org/user/login" , body = values )

	httr::GET( "http://www.europeansocialsurvey.org/user/login" , query = values )

    for ( i in seq_len( nrow( catalog ) ) ){

		# ignore special case of a file containing two datasets instead of one
		# (each dataset is downloaded separately by other catalog entries)
		if( grepl( "ESS3LVRO" , catalog[ i , 'full_url' ] ) ) {
			next
		}

		# download the file
		current.file <- cachaca( catalog[ i , 'full_url' ] , FUN = httr::GET )

		writeBin( httr::content( current.file ) , tf )

		if( !grepl( "\\.pdf$" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){
			spss.files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )
		} else {
			spss.files <- tf
		}
		
		if( catalog[ i , 'directory' ] == 'docs' ){
		
			file.copy( spss.files , catalog[ i , 'output_filename' ] )
			
		} else {
			
			# first, look for the .sav file
			if ( any( grepl( 'sav' , spss.files ) ) ){
			
				# read that dot.sav file as a data.frame object
				if( grepl( "ESS7occpCZ|ESS6occpSK" , spss.files[ grep( 'sav' , spss.files ) ] ) ){
					x <- foreign::read.spss( spss.files[ grep( 'sav' , spss.files ) ] , to.data.frame = TRUE , use.value.labels = FALSE )
				} else {
					x <- data.frame( haven::read_spss( spss.files[ grep( 'sav' , spss.files ) ] ) )
				}
				
			} else {
			
				# otherwise, read in from the `.por` file
				attempt.one <- 
					try( 
						# read that dot.por file as a data.frame object
						suppressWarnings( x <- foreign::read.spss( spss.files[ grep( 'por' , spss.files ) ] , to.data.frame = TRUE , use.value.labels = FALSE ) ) ,
						silent = TRUE
					)
					
				# if the prior attempt failed..
				if ( class( attempt.one ) == 'try-error' ){
					# otherwise, convert all factor variables to character
					attempt.two <- 
						try( 
							# use the `memisc` package's `spss.portable.file` framework instead
							x <-
								data.frame(
									memisc::as.data.set(
										memisc::spss.portable.file( 
											spss.files[ grep( 'por' , spss.files ) ] 
										)
									)
								) ,
							silent = TRUE
						)
						
				} else attempt.two <- NULL
				
				
				# if the prior attempt failed..
				if ( class( attempt.two ) == 'try-error' ){
				
					# use the `memisc` package's `spss.portable.file` framework instead
					b <-
						memisc::as.data.set(
							memisc::spss.portable.file( 
								spss.files[ grep( 'por' , spss.files ) ] 
							)
						)
					
					# convert all factor variables to character variables
					b <- sapply( b , function( z ) { if( class( z ) == 'factor' ) z <- as.character( z ) ; z } )
					
					# now run the conversion that caused the issue.
					x <- data.frame( b )
				
				}
						
			}
		
			# delete the temporary files
			file.remove( spss.files , tf )
		
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

		}
		
		cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
		
    }

	on.exit()
	
    catalog

  }

