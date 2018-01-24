get_catalog_addhealth <-
	function( data_name = "addhealth" , output_dir , ... ){

	catalog <- get_catalog_icpsr( study_numbers = "21600" , bundle_preference = "rdata" , archive = "DSDR" )
	
	catalog$wave <- tolower( stringr::str_trim( gsub( "[[:punct:]]" , "" , sapply( strsplit( catalog$dataset_name , ":" ) , "[[" , 1 ) ) ) )
	
	catalog$data_title <- tolower( stringr::str_trim( gsub( "[[:punct:]]" , "" , sapply( strsplit( catalog$dataset_name , ":" ) , "[[" , 2 ) ) ) )
	
	catalog$unzip_folder <- paste0( output_dir , "/" , catalog$wave , "/" , catalog$data_title , "/" )
	
	catalog$output_folder <- paste0( output_dir , "/" , catalog$wave , "/" )

	catalog

}


lodown_addhealth <-
	function( data_name = "addhealth" , catalog , ... ){

		on.exit( print( catalog ) )

		catalog <- lodown_icpsr( data_name = data_name , catalog , ... )

				
		# loop through each of the available interview waves..
		for ( curWave in seq_along( unique( catalog$wave ) ) ){

			# extract the `.rda` files available for that wave
			rda_files_to_merge <- 
				unlist( 
					lapply( 
						catalog[ catalog$wave == unique( catalog$wave )[ curWave ] , 'unzip_folder' ] , 
						function( w ) grep( "rda$" , list.files( w , full.names = TRUE , recursive = TRUE ) , value = TRUE ) 
					) 
				)

			# create an empty `cons` object
			cons <- NULL	
			
			# loop through each of the appropriate `.rda` files
			for ( this_rda in rda_files_to_merge ){
			
				# load it into RAM
				df_name <- load( this_rda )
				
				# make sure it's called `x`
				if( df_name != 'x' ) { x <- get( df_name ) ; rm( list = df_name ) ; gc() }
				
				names( x ) <- tolower( names( x ) )
				
				# confirm the file must be one-record-per-unique ID
				if ( length( unique( x$aid ) ) == nrow( x ) ){
				
					# print current progress to the screen
					cat( paste( "currently merging" , this_rda , "from wave" , curWave , "\r                               " ) )
			
					if ( !grepl( 'weight' , this_rda ) ) x$cluster2 <- NULL
			
					# if the `cons` object is missing..
					if ( is.null( cons ) ){
						
						# it's the first data.frame to be included in the consolidated file
						cons <- x
					
					# otherwise
					} else {
					
						# copy over what's already in the `cons` object
						pre.cons <- cons
						
						# if the unique identifier is available,
						# don't also merge on caseid.
						if ( 'aid' %in% names( cons ) ) cons$caseid <- NULL
						
						# print what you're doing, just to keep everyone abreast of current inner-workings.
						cat( paste0( paste( "merging with" , intersect( names( x ) , names( cons ) ) , collapse = " and " ) , "\r                                  " ) )
					
						# merge the current .rda with what's already in `cons`,
						# keeping matching records in *either* data set
						cons <- merge( cons , x , all = TRUE )
						
					}
					
					# make sure the many-to-one merge hasn't gone apeshit.
					# none of these should have more than ten thousand records ever
					stopifnot( nrow( cons ) < 10000 )
					
				} else {
				
					# otherwise no merge..
					cat( paste( "did not merge" , this_rda , " -- copying to working directory" , "\r                                  " ) )
					
					# just save the data.frame object into the main output folder
					saveRDS( x , file = gsub( "/individual tables" , "" , gsub( "\\.rda" , ".rds" , this_rda ) ) , compress = FALSE )
				}
				
				# remove the current data.frame from working memory
				rm( x )
				
				# clear up RAM
				gc()
			}
			
			
			consolidated_filename <- 
				paste0( 
					unique( catalog[ catalog$wave == unique( catalog$wave )[ curWave ] , 'output_folder' ] ) , 
					unique( catalog$wave )[ curWave ] , 
					' consolidated.rds' 
				)
			
			# once you've merged as many files as you can,
			# save the final `cons` object to the local disk
			saveRDS( cons , file = consolidated_filename , compress = FALSE )
			
			cat( paste0( data_name , " consolidated file stored at '" , consolidated_filename , "'\r                                  " ) )

			# remove the `cons` object from working memory
			rm( cons )
			
			# once again, clear up RAM
			gc()
		}

		on.exit()
		
		catalog

	}

