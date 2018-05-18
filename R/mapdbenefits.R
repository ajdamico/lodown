get_catalog_mapdbenefits <-
  function( data_name = "mapdbenefits" , output_dir , ... ){

	cpsc_url <- "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/Benefits-Data.html"

	all_dates <- rvest::html_table(xml2::read_html(cpsc_url))
	all_titles <- all_dates[[1]][ , "Title"]
	all_dates <- all_dates[[1]][ , "Report Period"]
	
	all_dates[ all_dates >= 2018 ] <-
		gsub( "PBP Benefits |- " , "" , all_titles[ all_dates >= 2018 ] )
	
	all_links <- rvest::html_nodes(xml2::read_html(cpsc_url),xpath='//td/a')

	prefix <- "https://www.cms.gov/"

	these_links <- 
		gsub( "\">(.*)" , "" , 
		gsub( '<a href=\"' , prefix , 
			all_links ) )

    this_catalog <-
      data.frame(
          output_folder = paste0( output_dir , "/" , all_dates ) ,
          full_url = as.character( these_links ) ,
          year_quarter = all_dates ,
		  stringsAsFactors = FALSE
      )
	
	for( this_row in seq( nrow( this_catalog ) ) ){
		
		link_text <- readLines( this_catalog[ this_row , 'full_url' ] )
		link_line <- grep( "zip" , link_text , value = TRUE )
		link_line <- gsub( '(.*) href=\"' , "" , gsub( '(.*) href=\"/' , prefix , link_line ) )
		this_catalog[ this_row , 'full_url' ] <- gsub( '\">(.*)' , "" , link_line )
		
	}
	
	
	this_catalog[ order( this_catalog$year ) , ]
  }


lodown_mapdbenefits <-
	function( data_name = "mapdbenefits" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = np_dirname( catalog[ i , 'output_folder' ] ) )

			txt_files <- grep( "\\.txt$" , unzipped_files , value = TRUE )

			txt_files <- txt_files[ !grepl( "^readme" , basename( txt_files ) , ignore.case = TRUE ) ]

			txt_files <- sort( txt_files )

			for( j in seq_along( txt_files ) ){

				x <- data.frame( readr::read_delim( txt_files[ j ] , delim = "\t" , guess_max = 10000 ) )

				names( x ) <- tolower( names( x ) )

				# confirm the first column is always the contract_id
				stopifnot( grepl( "_hnumber" , names( x )[ 1 ] ) )
				
				# confirm the second column is always the plan_id
				stopifnot( grepl( "plan_identifier" , names( x )[ 2 ] ) )
				
				names( x )[ 1:2 ] <- c( 'contract_id' , 'plan_id' )
				
				x[ , 2 ] <- as.numeric( x[ , 2 ] )
				
				saveRDS( 
					x , 
					file = 
						file.path( 
							catalog[ i , 'output_folder' ] , 
							gsub( "\\.txt" , ".rds" , basename( txt_files[ j ] ) ) 
						)
				)

				rm( x ) ; gc()

			}
			
			# copy over excel files (data dictionaries)
			file.copy( 
				grep( "\\.xls" , unzipped_files , value = TRUE ) , 
				file.path(
					catalog[ i , 'output_folder' ] ,
					basename( grep( "\\.xls" , unzipped_files , value = TRUE ) )
				)
			)
			
			file.remove( unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()

		catalog

	}

