get_catalog_pisa <-
	function( data_name = "pisa" , output_dir , ... ){

	
		http.pre <- "https://www.oecd.org/pisa/pisaproducts/"

		
		files_2015 <- c( "CMB_STU_QQQ" , "CMB_SCH_QQQ" , "CMB_TCH_QQQ" , "CMB_STU_COG" , "CMB_STU%20_QTM" , "CM2_STU_QQQ_COG_QTM_SCH_TCH" )
	
		cat_2015 <-
			data.frame(
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				db_tablename = tolower( files_2015 ) ,
				year = 2015 ,
				full_url = paste0( "http://vs-web-fs-1.oecd.org/pisa/PUF_SAS_COMBINED_" , files_2015 , ".zip" ) ,
				sas_ri = NA ,
				stringsAsFactors = FALSE
			)

			
		files_2012 <- c( "INT_STU12_DEC03", "INT_SCQ12_DEC03" ,  "INT_PAQ12_DEC03" , "INT_COG12_DEC03" , "INT_COG12_S_DEC03" )
		
		sas_2012 <- paste0( "PISA" , 2012 , "_SAS_" , c( "student" , "school" , "parent" , "cognitive_item" , "scored_cognitive_item" ) )
		
		cat_2012 <-
			data.frame(
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				db_tablename = tolower( files_2012 ) ,
				year = 2012 ,
				full_url = paste0( http.pre , files_2012 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2012 , ".sas" ) ,
				stringsAsFactors = FALSE
			)
			
		files_2009 <- c( "INT_STQ09_DEC11" , "INT_SCQ09_Dec11" , "INT_PAR09_DEC11" , "INT_COG09_TD_DEC11" , "INT_COG09_S_DEC11" )
	
		sas_2009 <- paste0( "PISA" , 2009 , "_SAS_" , c( "student" , "school" , "parent" , "cognitive_item" , "scored_cognitive_item" ) )
	
		cat_2009 <-
			data.frame(
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				db_tablename = tolower( files_2009 ) ,
				year = 2009 ,
				full_url = paste0( http.pre , files_2009 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2012 , ".sas" ) ,
				stringsAsFactors = FALSE
			)
			

		files_2006 <- c( "INT_Stu06_Dec07" , "INT_Sch06_Dec07" , "INT_Par06_Dec07" , "INT_Cogn06_T_Dec07" , "INT_Cogn06_S_Dec07" )

		sas_2006 <- paste0( "PISA" , 2006 , "_SAS_" , c( "student" , "school" , "parent" , "cognitive_item" , "scored_cognitive_item" ) )
		
		cat_2006 <-
			data.frame(
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				db_tablename = tolower( files_2006 ) ,
				year = 2006 ,
				full_url = paste0( http.pre , files_2006 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2006 , ".sas" ) ,
				stringsAsFactors = FALSE
			)
			
		
		files_2003 <- c( "INT_cogn_2003" , "INT_stui_2003_v2" , "INT_schi_2003" )
	
		sas_2003 <- paste0( "PISA" , 2003 , "_SAS_" , c( "cognitive_item" , "student" , "school" ) )
	
		cat_2003 <-
			data.frame(
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				db_tablename = tolower( files_2003 ) ,
				year = 2003 ,
				full_url = paste0( http.pre , files_2003 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2003 , ".sas" ) ,
				stringsAsFactors = FALSE
			)
			
		files_2000 <- c( "intcogn_v4" , "intscho" , "intstud_math" , "intstud_read" , "intstud_scie" )

		sas_2000 <- paste0( "PISA" , 2000 , "_SAS_" , c( "cognitive_item" , "school_questionnaire" , "student_mathematics" , "student_reading" , "student_science" ) )
	
	
		cat_2000 <-
			data.frame(
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				db_tablename = tolower( files_2000 ) ,
				year = 2000 ,
				full_url = paste0( http.pre , files_2000 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2000 , ".sas" ) ,
				stringsAsFactors = FALSE
			)
			
			
			
		rbind( cat_2015 , cat_2012 , cat_2009 , cat_2006 , cat_2003 , cat_2000 )

	}


lodown_pisa <-
	function( data_name = "pisa" , catalog , ... ){

		if ( !requireNamespace( "mitools" , quietly = TRUE ) ) stop( "mitools needed for this function to work. to install it, type `install.packages( 'mitools' )`" , call. = FALSE )
		
		
		

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

			
			if( !is.na( catalog[ i , 'sas_ri' ] ) ) cachaca( catalog[ i , "sas_ri" ] , tf , mode = 'wb' )
			
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )






			# convert all column names to lowercase
			# names( x ) <- tolower( names( x ) )

			# save( x , file = catalog[ i , 'output_filename' ] )

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

