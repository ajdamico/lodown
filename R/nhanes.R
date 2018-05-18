get_catalog_nhanes <-
	function( data_name = "nhanes" , output_dir , ... ){

		data_page <- "https://wwwn.cdc.gov/nchs/nhanes/search/DataPage.aspx"
		
		data_html <- xml2::read_html( data_page )
		
		this_table <- rvest::html_table( data_html )[[2]]
	
		names( this_table ) <- c( 'years' , 'data_name' , 'doc_name' , 'file_name' , 'date_published' )

		
		all_links <- rvest::html_nodes( data_html , "a" )
		
		link_text <- rvest::html_text( all_links )
		
		link_refs <- rvest::html_attr( all_links , "href" )
		
		this_table$full_url <- link_refs[ match( this_table$file_name , link_text ) ]

		this_table$doc_url <- link_refs[ match( this_table$doc_name , link_text ) ]

		this_table[ c( 'full_url' , 'doc_url' ) ] <- sapply( this_table[ c( 'full_url' , 'doc_url' ) ] , function( w ) ifelse( is.na( w ) , NA , paste0( "https://wwwn.cdc.gov" , w ) ) )
		
		catalog <- this_table[ this_table$file_name != 'RDC Only' & this_table$date_published != 'Withdrawn' & this_table$full_url != "https://wwwn.cdc.gov#" , ]

		# one all years doc hardcode
		ayd <- catalog[ tolower( catalog$full_url ) == "https://wwwn.cdc.gov/nchs/nhanes/dxa/dxa.aspx" , ]
		
		ayd$years <- ayd$full_url <- ayd$doc_url <- NULL
		
		this_ayd <-
			data.frame(
				years = c( "2005-2006" , "2003-2004" , "2001-2002" , "1999-2000" ) ,
				full_url = paste0( "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Dxa/dxx" , c( "_d" , "_c" , "_b" , "" ) , ".xpt" ) ,
				doc_url = paste0( "https://wwwn.cdc.gov/Nchs/Nhanes/2005-2006/DXX_D.htm" , "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Dxa/dxx_c.pdf" , "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Dxa/dxx_b.pdf" , "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Dxa/dxx.pdf" ) ,
				stringsAsFactors = FALSE
			)
		
		ayd <- merge( ayd , this_ayd )
		
		catalog <- catalog[ tolower( catalog$full_url ) != "https://wwwn.cdc.gov/nchs/nhanes/dxa/dxa.aspx" , ]
		
		catalog <- rbind( catalog , ayd )

		catalog$output_filename <- paste0( output_dir , "/" , catalog$years , "/" , tolower( gsub( "\\.xpt" , ".rds" , basename( catalog$full_url ) , ignore.case = TRUE ) ) )
		
		catalog <- catalog[ order( catalog[ , 'years' ] ) , ]
		
		catalog

	}


lodown_nhanes <-
	function( data_name = "nhanes" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			if( grepl( "\\.zip$" , catalog[ i , "full_url" ] , ignore.case = TRUE ) ){
				
				unzipped_files <- unzip( tf , exdir = tempdir() )
				
				suppressWarnings( file.remove( tf ) )
				
				tf <- unzipped_files

			}
			
			xport_attempt <- try( x <- foreign::read.xport( tf ) , silent = TRUE )
			
			if( class( xport_attempt ) == 'try-error' ) x <- data.frame( haven::read_sas( tf ) )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

