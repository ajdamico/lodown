get_catalog_sipp <-
	function( data_name = "sipp" , output_dir , ... ){
	
		cat_1996 <-
			data.frame(
				panel = 1996 ,
				wave = c( 1:12 , 1:12 , 1:12 , rep( NA , 6 ) ) ,
				cy = c( rep( NA , 37 ) , 1996:1999 , NA ) ,
				db_tablename = c( paste0( "w" , 1:12 ) , paste0( "rw" , 1:12 ) , paste0( "tm" , 1:12 ) , "wgtw12" , paste0( 'cy' , 1:4 ) , "pnl" ) ,
				full_url =
					c(
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/1996/l96puw" , 1:12 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/1996/rw96w" , 1:12 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/1996/tm96puw" , 1:9 , ".zip" ) , 
						"http://thedataweb.rm.census.gov/pub/sipp/1996/p96putm10.zip" ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/1996/tm96puw" , 11:12 , ".zip" ) ,
						"http://thedataweb.rm.census.gov/pub/sipp/1996/ctl_fer.zip" ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/1996/lrw96" , c( paste0( 'cy' , 1:4 ) , 'pnl' ) , ".zip" )
					) ,
				dbfolder = paste0( output_dir , "/MonetDB_1996" ) ,
				stringsAsFactors = FALSE
			)
						
		cat_2001 <-
			data.frame(
				panel = 2001 ,
				wave = c( 1:9 , 1:9 , 1:9 , rep( NA , 9 ) ) ,
				cy = c( rep( NA , 30 ) , 2001:2003 , 2001:2003 ) ,
				db_tablename = c( paste0( "w" , 1:9 ) , paste0( "rw" , 1:9 ) , paste0( "tm" , 1:9 ) , "wgtw9" , "hh" , "wf" , paste0( 'cy' , 1:3 ) , paste0( 'pnl' , 1:3 ) ) ,
				full_url =
					c(
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2001/l01puw" , 1:9 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2001/rw01w" , 1:9 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2001/p01putm" , 1:9 , ".zip" ) ,
						"http://thedataweb.rm.census.gov/pub/sipp/2001/lgtwgt2001w9.zip" ,
						"http://thedataweb.rm.census.gov/pub/sipp/2001/hhldpuw1.zip" ,
						"http://thedataweb.rm.census.gov/pub/sipp/2001/p01putm8x.zip" ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2001/lgtwgt" , c( paste0( 'cy' , 1:3 ) , paste0( 'pnl' , 1:3 ) ) , ".zip" )
					) ,
				dbfolder = paste0( output_dir , "/MonetDB_2001" ) ,
				stringsAsFactors = FALSE
			)
						
				
		cat_2004 <-
			data.frame(
				panel = 2004 ,
				wave = c( 1:12 , 1:12 , 1:8 , rep( NA , 11 ) ) ,
				cy = c( rep( NA , 33 ) , 2004:2007 , 2004:2007 , NA , NA ) ,
				db_tablename = c( paste0( "w" , 1:12 ) , paste0( "rw" , 1:12 ) , paste0( "tm" , 1:8 ) , "wgtw12" , paste0( 'cy' , 1:4 ) , paste0( 'pnl' , 1:4 ) , 'aoa3' , 'aoa6' ) ,
				full_url =
					c(
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2004/l04puw" , 1:12 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2004/rw04w" , 1:12 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2004/p04putm" , 1:8 , ".zip" ) ,
						"http://thedataweb.rm.census.gov/pub/sipp/2004/lgtwgt2004w12.zip" ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2004/lrw04_" , c( paste0( 'cy' , 1:4 ) , paste0( 'pnl' , 1:4 )  ) , ".zip" ) ,
						"http://thedataweb.rm.census.gov/pub/sipp/2004/p04putm3_aoa.zip" ,
						"http://thedataweb.rm.census.gov/pub/sipp/2004/p04putm6_aoa.zip"
					) ,
				dbfolder = paste0( output_dir , "/MonetDB_2004" ) ,
				stringsAsFactors = FALSE
			)
						
		
		cat_2008 <-
			data.frame(
				panel = 2008 ,
				wave = c( 1:16 , 1:16 , 1:11 , 13 , rep( NA , 11 ) ) ,
				cy = c( rep( NA , 45 ) , 2009:2013 , 2009:2013 ) ,
				db_tablename = c( paste0( "w" , 1:16 ) , paste0( "rw" , 1:16 ) , paste0( "tm" , c( 1:11 , 13 ) ) , "wgtw16" , paste0( 'cy' , 1:5 ) , paste0( 'pn' , 1:5 ) ) ,
				full_url =
					c(
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2008/l08puw" , 1:16 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2008/rw08w" , 1:16 , ".zip" ) ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2008/p08putm" , c( 1:11 , 13 ) , ".zip" ) ,
						"http://thedataweb.rm.census.gov/pub/sipp/2008/lgtwgt2008w16.zip" ,
						paste0( "http://thedataweb.rm.census.gov/pub/sipp/2008/lrw08" , c( paste0( 'cy' , 1:5 ) , paste0( 'pn' , 1:5 ) ) , ".zip" )
					) ,
				dbfolder = paste0( output_dir , "/MonetDB_2008" ) ,
				stringsAsFactors = FALSE
			)
						
		
		rbind( cat_1996 , cat_2001 , cat_2004 , cat_2008 )

	}


lodown_sipp <-
	function( data_name = "sipp" , catalog , ... ){

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )






			# convert all column names to lowercase
			# names( x ) <- tolower( names( x ) )

			# save( x , file = catalog[ i , 'output_filename' ] )

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}










