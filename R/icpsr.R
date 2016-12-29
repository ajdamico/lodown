#' locally download, import, prepare icpsr microdata
#'
#' get_catalog_icpsr retrieves a listing of all available extracts for a microdata set
#'
#' @param data_name a character vector with a microdata abbreviation
#' @param catalog \code{data.frame} detailing available microdata extracts
#' @param series_number any data series number from http://www.icpsr.umich.edu/icpsrweb/ICPSR/series
#' @param study_numbers one or more study numbers from http://www.icpsr.umich.edu/icpsrweb/ICPSR/studies?q=
#' @param archive defaults to "ICPSR" but other archives include "HMCA" , "NACJD" , "NAHDAP" , "NADAC" , "civicleads" , "RCMD" , "ADDEP" , "childcare" , "NCAA" , "NACDA" , "DSDR" , "METLDB"
#' @param bundle_preference when multiple filetypes are available, which should be given priority?
#' @param ... passed to \code{get_catalog} and \code{lodown_}
#'
#' @export
lodown_icpsr <-
  function( data_name , catalog , ... ){

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at https://www.icpsr.umich.edu/cgi-bin/newacct" )
		
		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://www.icpsr.umich.edu/cgi-bin/newacct" )

		your_email <- list(...)[["your_email"]]
		
		your_password <- list(...)[["your_password"]]

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){
	
			# initiate a curl handle so the remote server knows it's you.
			curl = RCurl::getCurlHandle()

			# set a cookie file on the local disk
			RCurl::curlSetOpt(
				cookiejar = 'cookies.txt' , 
				followlocation = TRUE , 
				autoreferer = TRUE , 
				curl = curl
			)
		
			# list out the filepath on the server of the file-to-download
			dp <- catalog[ i , 'full_url' ]
				
			# post your username and password to the umich server
			login.page <- 
				RCurl::postForm(
					"https://www.icpsr.umich.edu/rpxlogin" , 
					email = your_email ,
					password = your_password ,
					path = catalog[ i , 'archive' ] ,
					request_uri = dp ,
					app_seq = "" ,
					style = "POST" ,
					curl = curl 
				)
		
			# consent to terms of use page
			terms.of.use.page <- 
				RCurl::postForm(
					"http://www.icpsr.umich.edu/cgi-bin/terms" , 
					agree = 'yes' ,
					path = catalog[ i , 'archive' ] , 
					study = gsub( "(.*)study=([0-9]+)&(.*)" , "\\2" , catalog[ i , 'full_url' ] ) , 
					bundle = catalog[ i , 'bundle' ] , 
					dups = "yes" ,
					style = "POST" ,
					curl = curl
				)
			
			cachaca( dp , tf , FUN = download_to_filename, curl=curl, filesize_fun = 'unzip_verify' )

			unzip( tf , exdir =  gsub( "/$" , "" , catalog[ i , "unzip_folder" ] ) , junkpaths = TRUE )

			# delete the temporary files
			file.remove( tf )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " unzipped to '" , catalog[ i , 'unzip_folder' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}



	
#' @rdname lodown_icpsr
#' @export
get_catalog_icpsr <-
	function( series_number = NULL , study_numbers = NULL , archive = "ICPSR" , bundle_preference = c( "rdata" , "stata" , "sas" , "spss" , "delimited" , "ascsas" , "ascstata" , "ascspss" , "ascii" ) ){

		if( is.null( study_numbers ) ){
		
			series_page <- paste0( "http://www.icpsr.umich.edu/icpsrweb/ICPSR/series/" , series_number , "/studies?archive=" , archive , "&sortBy=7" )

			series_xml <- xml2::read_html( series_page )

			study_numbers <- gsub( "[^0-9]" , "" , rvest::html_text( rvest::html_nodes( series_xml , ".studyNo" ) ) )
		
		}
		
		series_results <- NULL

		for( study_number in study_numbers ){

			study_page <- paste0( "http://www.icpsr.umich.edu/icpsrweb/ICPSR/" , if( !is.null( series_number ) ) paste0( "series/" , series_number ) , "/studies/" , study_number , "?archive=" , archive , "&sortBy=7" )

			study_xml <- xml2::read_html( study_page )

			study_json <- rvest::html_text( rvest::html_nodes( study_xml , 'script[type$="json"]' ) )

			if( !grepl( "No downloadable data files available." , rvest::html_text( study_xml ) ) ){

				json_result <- jsonlite::fromJSON( gsub( "\n|\r" , " " , study_json ) , simplifyDataFrame = TRUE )

				dataset_xml <- rvest::html_nodes( study_xml , 'div[class$=datasetDownload]' )
				
				dataset_names <- gsub( "\n|\t" , "" , rvest::html_text( rvest::html_nodes( dataset_xml , 'strong' ) ) )
				
				dataset_ids <- sapply( rvest::html_attrs( rvest::html_nodes( study_xml , "div[id^=dataset]" ) ) , '[[' , 2 )
				
				dataset_ids <- gsub( "dataset" , "" , grep( "dataset" , dataset_ids , value = TRUE ) )
				
				available_bundles <- grep( "ds=" , rvest::html_attr( rvest::html_nodes( study_xml , "a" ) , "href" ) , value = TRUE )
				
				all_ds <- NULL
				
				for( this_id in dataset_ids ){
					
					this_bundle <- NULL
					
					pref_num <- 1
					
					while( is.null( this_bundle ) ){
					
						current_preference <- bundle_preference[ pref_num ]
						
						bundle_test <- grep( paste0( "ds=" , this_id , "&bundle=" , current_preference ) , available_bundles , value = TRUE )
						
						if( length( bundle_test ) == 1 ) this_bundle <- bundle_test else pref_num <- pref_num + 1
						
						if( pref_num > length( bundle_preference ) ) break
					
					}
					
					if( !is.null( this_bundle ) ) all_ds <- rbind( all_ds , data.frame( dataset_name = dataset_names[ which( this_id == dataset_ids ) ] , ds = this_id , full_url = this_bundle , stringsAsFactors = FALSE ) )
					
				}

				all_ds$dataset_name <- stringr::str_trim( gsub( "\\n|\\t|\\r|DS([0-9]+):" , "" , all_ds$dataset_name ) )
				
				json_result$distribution <- NULL
				
				multiples <- lapply( json_result , length ) > 1
				
				json_result[ multiples ] <- lapply( json_result[ multiples ] , function( z ) paste( z , collapse = " " ) )
				
				this_study <- as.data.frame( json_result , stringsAsFactors = FALSE )

				stopifnot( nrow( this_study ) == 1 )

				if( !( 'funder' %in% names( this_study ) ) ) this_study$funder <- NA

				this_study <- merge( this_study , all_ds )
				
				series_results <- rbind( series_results , this_study )
			
			}
			
		}
		
		series_results$archive <- gsub( "(.*)icpsrweb/(.*)/(.*)" , "\\2" , series_results$includedInDataCatalog )
		
		series_results$bundle <- gsub( "(.*)bundle=(.*)&(.*)" , "\\2" , series_results$full_url ) 
		
		series_results[ series_results$bundle == 'sas' , 'bundle' ] <- 'ascsas'
		
		series_results
	
	}
