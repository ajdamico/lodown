get_catalog_pew <-
	function( data_name = "pew" , output_dir , ... ){
			
		catalog <- NULL

		dl_homepage <- "http://www.pewresearch.org/data/download-datasets/"

		# figure out research areas #
		research_areas <- xml2::read_html( dl_homepage )

		ra_link_refs <- rvest::html_attr( rvest::html_nodes( research_areas , "a" ) , "href" )
			
		ra_link_text <- rvest::html_text( rvest::html_nodes( research_areas , "a" ) )

		ra_link_text <- ra_link_text[ grep( "/datasets/" , ra_link_refs ) ]

		ra_link_refs <- ra_link_refs[ grep( "/datasets/" , ra_link_refs ) ]


		for( topic_num in seq_along( ra_link_text ) ){

			# figure out years #
			topic_page <- xml2::read_html( ra_link_refs[ topic_num ] )

			to_link_refs <- rvest::html_attr( rvest::html_nodes( topic_page , "a" ) , "href" )
				
			to_link_text <- rvest::html_text( rvest::html_nodes( topic_page , "a" ) )

			year_link_refs <- to_link_refs[ grep( "^[0-9][0-9][0-9][0-9]$" , to_link_text ) ]

			year_link_text <- to_link_text[ grep( "^[0-9][0-9][0-9][0-9]$" , to_link_text ) ]

			for( year_num in seq_along( year_link_text ) ){

				# figure out pages #
				year_page <- xml2::read_html( year_link_refs[ year_num ] )

				data_link_refs <- rvest::html_attr( rvest::html_nodes( year_page , "a" ) , "href" )
					
				data_link_text <- rvest::html_text( rvest::html_nodes( year_page , "a" ) )

				page_list <- as.numeric( unique( gsub( "(.*)/page/([0-9]+)/$" , "\\2" , grep( "/page/[0-9]+/$" , data_link_refs , value = TRUE ) ) ) )
				
				if( length( page_list ) == 0 ) all_pages <- 1 else all_pages <- seq( max( page_list ) )

				
				for( page_num in all_pages ){

					# figure out datasets #
					these_data_webpage <- paste0( year_link_refs[ year_num ] , "/page/" , page_num , "/" )

					these_data_page <- xml2::read_html( these_data_webpage )

					these_data_link_link <- rvest::html_attr( rvest::html_nodes( these_data_page , "a" ) , "dataset-dl-link" )

					these_data_link_refs <- rvest::html_attr( rvest::html_nodes( these_data_page , "a" ) , "href" )

					these_data_link_title <- rvest::html_attr( rvest::html_nodes( these_data_page , "a" ) , "title" )
					
					these_data_link_text <- rvest::html_text( rvest::html_nodes( these_data_page , "a" ) )
					
					these_data_link_text <- these_data_link_text[ !is.na( these_data_link_link ) ]
					
					these_data_link_title <- these_data_link_title[ !is.na( these_data_link_link ) ]
					
					these_data_link_refs <- these_data_link_refs[ !is.na( these_data_link_link ) ]
					
					these_data_link_link <- these_data_link_link[ !is.na( these_data_link_link ) ]
					
					
					
					if( length( these_data_link_refs ) > 0 ){
						
						this_catalog <-
							data.frame(
								full_url = these_data_link_link ,
								name = these_data_link_title ,
								download_id = gsub( "(.*)\\((.*)\\)" , "\\2" , these_data_link_refs ) ,
								year = year_link_text[ year_num ] ,
								topic = ra_link_text[ topic_num ] ,
								stringsAsFactors = FALSE
							)
						
						this_catalog[ this_catalog$topic == this_catalog$year , 'year' ] <- gsub( "(.*)([0-9][0-9][0-9][0-9])(.*)" , "\\2" , this_catalog[ this_catalog$topic == this_catalog$year , 'name' ] )
						
						# this_catalog[ !grepl( "^[0-9][0-9][0-9][0-9]$" , this_catalog$year ) , 'year' ] <- NA
						
						# keep only datasets with dl-links for now
						this_catalog <- subset( this_catalog , these_data_link_link != '' )
						
						catalog <- rbind( catalog , this_catalog )
						
						cat( paste0( "loading " , data_name , " catalog from " , these_data_webpage , "\r\n\n" ) )
					
					}
					
				}

			}

		}

		catalog$output_folder <- paste0( output_dir , "/" , catalog$topic , "/" , ifelse( !is.na( catalog$year ) , catalog$year , "" ) , "/" , gsub( "/|:|\\(|\\)" , "_" , catalog$name ) )
		
		catalog$output_folder <- gsub( " +" , " " , iconv( catalog$output_folder , "" , "ASCII//TRANSLIT" , sub = " " ) )
		
		catalog$output_folder <- gsub( 'a\\?|\\"' , '' , catalog$output_folder )
		
		# broken zips
		catalog <- 
			catalog[ 
				!( catalog$full_url %in% 
					c( 
					"http://assets.pewresearch.org/wp-content/uploads/sites/11/2015/12/Religion-in-Latin-America-Dataset.zip" , 
					"http://www.people-press.org/files/datasets/Jan%2030-Feb%202%202014%20omnibus.zip" , 
					"http://www.pewforum.org/datasets/a-portrait-of-jewish-americans/?submitted" ,
					'http://assets.pewresearch.org/wp-content/uploads/sites/5/datasets/Jan%2030-Feb%202%202014%20omnibus.zip' ,
					'http://assets.pewresearch.org/wp-content/uploads/sites/5/datasets/Oct+27-30+2011+omnibus.zip' ,
					'http://assets.pewresearch.org/wp-content/uploads/sites/5/datasets/Oct16.zip' ,
					'http://assets.pewresearch.org/wp-content/uploads/sites/14/2015/05/November-2010-–-Facebook-and-Social-Support.zip' ,
					'http://www.people-press.org/files/datasets/Aug16.zip' ,
					'http://assets.pewresearch.org/wp-content/uploads/sites/14/old-datasets/November-2010--Paid-Content-(Omnibus).zip' ,
					'http://assets.pewresearch.org/wp-content/uploads/sites/5/datasets/June16%20public.zip' ,
					
					'http://assets.pewresearch.org/wp-content/uploads/sites/2/2017/07/20111442/Pew-GAP-Spring-2007-survey-for-website.zip' ,
					'http://assets.pewresearch.org/wp-content/uploads/sites/2/2009/06/Pew-GAP-Spring-2009-survey-for-website.zip'
				) ) , ]
		
		catalog

	}


lodown_pew <-
	function( data_name = "pew" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()
		
		for ( i in seq_len( nrow( catalog ) ) ){

			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )
			
			if( grepl( "\\.zip$" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){
				
				unzipped_files <- unzip_warn_fail( tf , exdir = catalog[ i , "output_folder" ] , junkpaths = TRUE )

				macosx <- grep( "\\._" , unzipped_files , value = TRUE )
				
				file.remove( macosx )
				
				unzipped_files <- unzipped_files[ !( unzipped_files %in% macosx ) ]
				
				sav_files <- grep( "\\.sav$" , unzipped_files , ignore.case = TRUE , value = TRUE )
							
			} else {
				
				sav_files <- paste0( catalog[ i , "output_folder" ] , "/" , gsub( "%20" , " " , basename( catalog[ i , 'full_url' ] ) ) )
				
				file.copy( tf , sav_files )
				
			}

			if( length( sav_files ) == 0 ){
				
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " unzipped in '" , catalog[ i , 'output_folder' ] , "' but zero spss files to import\r\n\n" ) )
				
			} else {
				
				for( this_sav in sav_files ){

					if( catalog[ i , 'full_url' ] == 'http://assets.pewresearch.org/wp-content/uploads/sites/2/2009/09/Pew-GAP-Fall-2009-BW-survey-for-website.zip' ){
						x <- data.frame( haven::read_spss( this_sav , encoding = "WINDOWS-1250" ) )
					} else {
						x <- data.frame( haven::read_spss( this_sav ) )
					}

					# convert all column names to lowercase
					names( x ) <- tolower( names( x ) )

					catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )
					
					saveRDS( x , file = gsub( "\\.sav$" , ".rds" , this_sav , ignore.case = TRUE ) , compress = FALSE )

				}
					
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

			}
			
			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

		}

		on.exit()
		
		catalog

	}

