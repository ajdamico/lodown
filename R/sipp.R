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
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/l96puw" , 1:12 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/rw96w" , 1:12 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/tm96puw" , 1:9 , ".zip" ) , 
						"https://thedataweb.rm.census.gov/pub/sipp/1996/p96putm10.zip" ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/tm96puw" , 11:12 , ".zip" ) ,
						"https://thedataweb.rm.census.gov/pub/sipp/1996/ctl_fer.zip" ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/lrw96" , c( paste0( 'cy' , 1:4 ) , 'pnl' ) , ".zip" )
					) ,
				dbfile = paste0( output_dir , "/SQLite_1996.db" ) ,
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
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/l01puw" , 1:9 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/rw01w" , 1:9 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/p01putm" , 1:9 , ".zip" ) ,
						"https://thedataweb.rm.census.gov/pub/sipp/2001/lgtwgt2001w9.zip" ,
						"https://thedataweb.rm.census.gov/pub/sipp/2001/hhldpuw1.zip" ,
						"https://thedataweb.rm.census.gov/pub/sipp/2001/p01putm8x.zip" ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/lgtwgt" , c( paste0( 'cy' , 1:3 ) , paste0( 'pnl' , 1:3 ) ) , ".zip" )
					) ,
				dbfile = paste0( output_dir , "/SQLite_2001.db" ) ,
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
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/l04puw" , 1:12 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/rw04w" , 1:12 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm" , 1:8 , ".zip" ) ,
						"https://thedataweb.rm.census.gov/pub/sipp/2004/lgtwgt2004w12.zip" ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/lrw04_" , c( paste0( 'cy' , 1:4 ) , paste0( 'pnl' , 1:4 )  ) , ".zip" ) ,
						"https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm3_aoa.zip" ,
						"https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm6_aoa.zip"
					) ,
				dbfile = paste0( output_dir , "/SQLite_2004.db" ) ,
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
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/l08puw" , 1:16 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/rw08w" , 1:16 , ".zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/p08putm" , c( 1:11 , 13 ) , ".zip" ) ,
						"https://thedataweb.rm.census.gov/pub/sipp/2008/lgtwgt2008w16.zip" ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/lrw08" , c( paste0( 'cy' , 1:5 ) , paste0( 'pn' , 1:5 ) ) , ".zip" )
					) ,
				dbfile = paste0( output_dir , "/SQLite_2008.df" ) ,
				stringsAsFactors = FALSE
			)

		cat_2014 <-
			data.frame(
				panel = 2014 ,
				wave = c( 1 , 1 ) ,
				cy = c( NA , NA ) ,
				db_tablename = c( paste0( "w" , 1 ) , paste0( "rw" , 1 ) ) ,
				full_url =
					c(
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2014/pu2014w" , 1 , "_dat.zip" ) ,
						paste0( "https://thedataweb.rm.census.gov/pub/sipp/2014/rw14w" , 1 , ".zip" ) 
					) ,
				dbfile = paste0( output_dir , "/SQLite_2014.db" ) ,
				stringsAsFactors = FALSE
			)
			
		
		rbind( cat_1996 , cat_2001 , cat_2004 , cat_2008 , cat_2014 )

	}


lodown_sipp <-
	function( data_name = "sipp" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfile' ] )


			if( catalog[ i , 'panel' ] == 1996 ){
				
				if ( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/1996/ctl_fer.zip" ){

					# the census SIPP FTP site does not have a SAS input script,
					# so create one using the dictionary at
					# https://thedataweb.rm.census.gov/pub/sipp/1996/ctl_ferd.asc

					# write an example SAS import script using the dash method
					sas.import.with.at.signs <-
						"INPUT
							@1 	   LGTKEY      8.
							@9      SPANEL       4.
							@13      SSUID      12.
							@25      EPPPNUM      4.
							@29      LGTPNLWT   10.
							@39      LGTPNWT1   10.
							@49      LGTPNWT2   10.
							@59      LGTPNWT3   10.
							@69      LGTPNWT4   10.
						;"
						
					# create a temporary file
					sas.import.with.at.signs.tf <- tempfile()
					# write the sas code above to that temporary file
					writeLines ( sas.import.with.at.signs , con = sas.import.with.at.signs.tf )

					# end of fake SAS input script creation #
					
					# add the longitudinal weights to the database in a table 'w12'
					read_SAScii_monetdb(
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( sas.import.with.at.signs.tf ) ) ,
						# note no beginline = parameter in this read_SAScii_monetdb() call
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
				
					
				
				if( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/l96puw" , 1:12 , ".zip" ) ){

					# add the core wave to the database in a table w#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/1996/sip96lgt.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
						
				}

				if( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/rw96w" , 1:12 , ".zip" ) ){

					# add the wave-specific replicate weight to the database in a table rw#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( fix.repwgt( "https://thedataweb.rm.census.gov/pub/sipp/1996/rw96wx.sas" ) ) ) ,
						beginline = 7 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)

				}

				if( 
					catalog[ i , 'full_url' ] %in% 
						c( 
							paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/tm96puw" , 1:9 , ".zip" ) , 
							"https://thedataweb.rm.census.gov/pub/sipp/1996/p96putm10.zip" ,
							paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/tm96puw" , 11:12 , ".zip" )
						) ){
				
					# add each topical module to the database in a table tm#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/p96putm" , catalog[ i , 'wave' ] , ".sas" ) ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
				
				
				if( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/1996/lrw96" , c( paste0( 'cy' , 1:4 ) , 'pnl' ) , ".zip" ) ){

					# add each longitudinal replicate weight file to the database in a table cy1-4 or pnl
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.repwgt( "https://thedataweb.rm.census.gov/pub/sipp/1996/lrw96_xx.sas" ) ) ,
						beginline = 7 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
				# the current working directory should now contain one database (.db) file

			}
				
			
			if( catalog[ i , 'panel' ] == 2001 ){

				if ( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2001/hhldpuw1.zip" ){

					# the census SIPP FTP site does not have a SAS input script,
					# so create one using the dictionary at
					# https://thedataweb.rm.census.gov/pub/sipp/2001/hhpuw1d.txt

					# write an example SAS import script using the dash method
					sas.import.with.at.signs <-
						"INPUT
							@1   SSUID   12.
							@13   UHOWLGMT  $ 2.
							@15   UHOWLGYR  $ 4.
							@19   UWHNAPMT  $ 2.
							@21   UWHNAPYR  $ 4.
							@25   UWAITLST  $ 1.
						;"
						
					# create a temporary file
					sas.import.with.at.signs.tf <- tempfile()
					# write the sas code above to that temporary file
					writeLines ( sas.import.with.at.signs , con = sas.import.with.at.signs.tf )

					# end of fake SAS input script creation #
					
					# add the longitudinal weights to the database in a table 'hh' (household)
					read_SAScii_monetdb(
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( sas.import.with.at.signs.tf ) ) ,
						# note no beginline = parameter in this read_SAScii_monetdb() call
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db ,
						try_best_effort = TRUE 
					)
					
				}
					
				# if the welfare reform module flag has been set to TRUE above..
				if ( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2001/p01putm8x.zip" ){

					# add the longitudinal weights to the database in a table 'wf' (welfare)
					read_SAScii_monetdb(
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2001/p01putm8x.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
					
				# if the longitudinal weights flag has been set to TRUE above..
				if ( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2001/lgtwgt2001w9.zip" ){

					# the census SIPP FTP site does not have a SAS input script,
					# so create one using the dictionary at
					# https://thedataweb.rm.census.gov/pub/sipp/2001/lgtwt01d.txt

					# write an example SAS import script using the dash method
					sas.import.with.at.signs <-
						"INPUT
							@1 	   LGTKEY      8.
							@9      SPANEL       4.
							@13      SSUID      12.
							@25      EPPPNUM      4.
							@29      LGTPNWT1   10.
							@39      LGTPNWT2   10.
							@49      LGTPNWT3   10.
							@59      LGTCY1WT   10.
							@69      LGTCY2WT   10.
							@79      LGTCY3WT   10.
						;"
						
					# create a temporary file
					sas.import.with.at.signs.tf <- tempfile()
					# write the sas code above to that temporary file
					writeLines ( sas.import.with.at.signs , con = sas.import.with.at.signs.tf )

					# end of fake SAS input script creation #
					
					# add the longitudinal weights to the database in a table 'w9'
					read_SAScii_monetdb(
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( sas.import.with.at.signs.tf ) ) ,
						# note no beginline = parameter in this read_SAScii_monetdb() call
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
					
				# loop through each core wave..
				if( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/l01puw" , 1:9 , ".zip" ) ){

					# add the core wave to the database in a table w#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2001/p01puw1.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}

				# loop through each replicate weight wave..
				if( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/rw01w" , 1:9 , ".zip" ) ){

					# add the wave-specific replicate weight to the database in a table rw#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2001/rw01wx.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)

				}

				# loop through each topical module..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/p01putm" , 1:9 , ".zip" ) ){
						
					# add each topical module to the database in a table tm#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/p01putm" , catalog[ i , 'wave' ] , ".sas" ) ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)

				}

				# loop through each longitudinal replicate weight file..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2001/lgtwgt" , c( paste0( 'cy' , 1:3 ) , paste0( 'pnl' , 1:3 ) ) , ".zip" ) ){

					# add each longitudinal replicate weight file to the database in a table cy1-3 or pnl1-3
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						fix.repwgt( "https://thedataweb.rm.census.gov/pub/sipp/2001/lrw01_xx.sas" ) ,
						beginline = 7 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
				# the current working directory should now contain one database (.db) file

			
			
			
			
			
			
			
			}
			
			if( catalog[ i , 'panel' ] == 2004 ){

				# if the longitudinal weights flag has been set to TRUE above..
				if ( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2004/lgtwgt2004w12.zip" ){

					# add the longitudinal weights to the database in a table 'w12'
					read_SAScii_monetdb(
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2004/lgtwgt2004w12.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
				}
					
				# loop through each core wave..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/l04puw" , 1:12 , ".zip" ) ){

					# add the core wave to the database in a table w#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2004/l04puw1.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}

				# loop through each replicate weight wave..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/rw04w" , 1:12 , ".zip" ) ){

					# add the wave-specific replicate weight to the database in a table rw#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2004/rw04wx.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}

				# loop through each topical module..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm" , 1:8 , ".zip" ) ){

					# add each topical module to the database in a table tm#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm" , catalog[ i , 'wave' ] , ".sas" ) ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}

				# add the two sipp assets extracts to the database
				if( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm3_aoa.zip" ){

					tm3_tablename <- catalog[ catalog$full_url == 'https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm3.zip' , 'db_tablename' ]
					
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid(fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm3_aoa.sas") ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)

					
					aoa3.fields <- DBI::dbListFields( db , catalog[ i , 'db_tablename' ] )
					aoa3.fields <- aoa3.fields[ !( aoa3.fields %in% c( 'ssuid' , 'epppnum' ) ) ]
					tm3.nis <- DBI::dbListFields( db , tm3_tablename )[ !( DBI::dbListFields( db , tm3_tablename ) %in% aoa3.fields ) ]
					tm3.ct <- 
						paste( 
							'create table temp_tm3 as select' ,
							paste( tm3.nis , collapse = " , " ) ,
							'from' , tm3_tablename
						)
					DBI::dbSendQuery( db , tm3.ct )
					DBI::dbRemoveTable( db , tm3_tablename )
					DBI::dbSendQuery( db , paste( 'create table' , tm3_tablename , 'as select * from temp_tm3' ) )
					DBI::dbRemoveTable( db , 'temp_tm3' )
					
				}
				
				if( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm6_aoa.zip" ){

					tm6_tablename <- catalog[ catalog$full_url == 'https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm6.zip' , 'db_tablename' ]
					
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid(fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2004/p04putm6_aoa.sas") ) ,
						beginline = 4 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)

					aoa6.fields <- DBI::dbListFields( db , catalog[ i , 'db_tablename' ] )
					aoa6.fields <- aoa6.fields[ !( aoa6.fields %in% c( 'ssuid' , 'epppnum' ) ) ]
					tm6.nis <- DBI::dbListFields( db , tm6_tablename )[ !( DBI::dbListFields( db , 'tm6' ) %in% aoa6.fields ) ]
					tm6.ct <- 
						paste( 
							'create table temp_tm6 as select' ,
							paste( tm6.nis , collapse = " , " ) ,
							'from' ,
							tm6_tablename
						)
					
					DBI::dbSendQuery( db , tm6.ct )
					DBI::dbRemoveTable( db , tm6_tablename )
					DBI::dbSendQuery( db , paste( 'create table' , tm6_tablename , 'as select * from temp_tm6' ) )
					DBI::dbRemoveTable( db , 'temp_tm6' )
					
				}

				# loop through each longitudinal replicate weight file..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2004/lrw04_" , c( paste0( 'cy' , 1:4 ) , paste0( 'pnl' , 1:4 )  ) , ".zip" ) ){

					# add each longitudinal replicate weight file to the database in a table cy1-4 or pnl1-4
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2004/lrw04_xx.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db  ,
						try_best_effort = ( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2004/lrw04_cy3.zip" )
					)
					
				}
				# the current working directory should now contain one database (.db) file


			
			
			
			
			}
			
			if( catalog[ i , 'panel' ] == 2008 ){



				# if the longitudinal weights flag has been set to TRUE above..
				if ( catalog[ i , 'full_url' ] == "https://thedataweb.rm.census.gov/pub/sipp/2008/lgtwgt2008w16.zip" ){

					# add the longitudinal weights to the database in a table 'wgtw14'
					read_SAScii_monetdb(
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2008/lgtwgt2008w16.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
					
				# loop through each core wave..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/l08puw" , 1:16 , ".zip" ) ){


					# add the core wave to the database in a table w#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2008/l08puw1.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}

				# loop through each replicate weight wave..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/rw08w" , 1:16 , ".zip" ) ){


					# add the wave-specific replicate weight to the database in a table rw#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2008/rw08wx.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}

				# loop through each topical module..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/p08putm" , c( 1:11 , 13 ) , ".zip" ) ){
					
					# add each topical module to the database in a table tm#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.ct( paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/p08putm" , catalog[ i , 'wave' ] , ".sas" ) ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}

				# loop through each longitudinal replicate weight file..
				if ( catalog[ i , 'full_url' ] %in% paste0( "https://thedataweb.rm.census.gov/pub/sipp/2008/lrw08" , c( paste0( 'cy' , 1:5 ) , paste0( 'pn' , 1:5 ) ) , ".zip" ) ){

					# add each longitudinal replicate weight file to the database in a table cy1-4 or pnl1-4
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						chop.suid( fix.repwgt( "https://thedataweb.rm.census.gov/pub/sipp/2008/lrw08_xx.sas" ) ) ,
						beginline = 7 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						connection = db 
					)
					
				}
				# the current working directory should now contain one database (.db) file


			}


			if( catalog[ i , 'panel' ] == 2014 ){

				# loop through each core wave..
				if ( grepl( "https://thedataweb.rm.census.gov/pub/sipp/2014/pu2014w" , catalog[ i , 'full_url' ] , fixed = TRUE ) ){

					# add the core wave to the database in a table w#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						fix.ahiehi( fix.ct( "https://thedataweb.rm.census.gov/pub/sipp/2014/pu2014w1.sas" ) ) ,
						beginline = 5 ,
						zipped = TRUE ,
						tl = TRUE ,
						na_strings = "." ,
						tablename = catalog[ i , 'db_tablename' ] ,
						unzip_fun = unarchive_nicely ,
						connection = db 
					)
					
				}
					
				# loop through each replicate weight wave..
				if ( grepl( "https://thedataweb.rm.census.gov/pub/sipp/2014/rw14w" , catalog[ i , 'full_url' ] , fixed = TRUE ) ){

					# add the core wave to the database in a table w#
					read_SAScii_monetdb (
						catalog[ i , 'full_url' ] ,
						fix.repwgt("https://thedataweb.rm.census.gov/pub/sipp/2014/rw14wx_sas.txt") ,
						beginline = 2 ,
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						unzip_fun = unarchive_nicely ,
						connection = db 
					)
					
				}

			}
			
			catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste( 'select count(*) from' , catalog[ i , 'db_tablename' ] ) )
			
			stopifnot( catalog[ i , 'case_count' ] > 0 )

			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "' table of " , catalog[ i , 'dbfile' ] , "'\r\n\n" ) )

		}

		DBI::dbDisconnect( db , shutdown = TRUE )
		
		on.exit()
		
		catalog

	}








##############################################################################
# function to fix sas input scripts where census has the incorrect column type
fix.ahiehi <-
	function( sasfile ){
		sas_lines <- readLines( sasfile )

		ahi_line <- which( sas_lines == "AHI1WHO   6923 - 6923" )
		ehi_line <- which( sas_lines == "EHI2WHO5   6922 - 6922" )
		
		seq_lines <- seq( length( sas_lines ) )
		
		seq_lines[ ahi_line ] <- ehi_line
		seq_lines[ ehi_line ] <- ahi_line
		
		sas_lines <- sas_lines[ seq_lines ]
		
		# create a temporary file
		tf <- tempfile()
		
		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
	}
##############################################################################

##############################################################################
# function to fix sas input scripts where census has the incorrect column type
fix.ct <-
	function( sasfile ){
		sas_lines <- readLines( sasfile )

		# ssuid should always be numeric (it's occasionally character)
		sas_lines <- gsub( "SSUID \\$" , "SSUID" , sas_lines )
		
		# ctl_date and lgtwttyp contain strings not numbers
		sas_lines <- gsub( "CTL_DATE" , "CTL_DATE $" , sas_lines )
		sas_lines <- gsub( "LGTWTTYP" , "LGTWTTYP $" , sas_lines )

		# create a temporary file
		tf <- tempfile()
		
		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
	}
##############################################################################

###################################################################################
# function to fix sas input scripts where repwgt values are collapsed into one line
# (the SAScii function cannot currently handle this SAS configuration on its own
fix.repwgt <-
	function( sasfile ){
		sas_lines <- readLines( sasfile )

		# identify the line containing REPWGT
		rep.position <- grep( "REPWGT" , sas_lines )
		
		# look at the line directly above it..
		line.above <- strsplit( sas_lines[ rep.position - 1 ] , "-" )[[1]]
		
		# ..and figure out what position it ends at
		end.position <- as.numeric( line.above[ length( line.above ) ] )
		
		# start with a line containing ()
		j <- sas_lines[ rep.position ]

		# courtesy of this discussion on stackoverflow.com
		# http://stackoverflow.com/questions/8613237/extract-info-inside-all-parenthesis-in-r-regex
		# break it into two strings without the ()
		k <- gsub( 
				"[\\(\\)]", 
				"" , 
				regmatches(
					j , 
					gregexpr( 
						"\\(.*?\\)" , 
						j
					)
				)[[1]]
			)

		# number of repweights
		l <- as.numeric( gsub( "REPWGT1-REPWGT" , "" , k )[1] )

		# length of repweights (assumes no decimals!)
		m <- as.numeric( k[2] )

		# these should start at the end position (determined above) plus one
		start.vec <- ( end.position + 1 ) + ( m * 0:( l - 1 ) )
		end.vec <- ( end.position ) + ( m * 1:l )
		
		
		# vector of all repweight lines
		repwgt.lines <-
			paste0( "REPWGT" , 1:l , " " , start.vec , "-" , end.vec )

		# collapse them all together into one string
		repwgt.line <- paste( repwgt.lines , collapse = " " )

		# finally replace the old line with the new line in the sas input script
		sas_lines <- gsub( j , repwgt.line , sas_lines , fixed = TRUE )
		
		# create a temporary file
		tf <- tempfile()
		
		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
	}
##################################################################################

##################################################################################
# sas importation scripts with an `SUID` column near the end
# are incorrect.  the census bureau just left them in,
# and the SAScii package won't just throw 'em out for ya.
# so throw out the non-public lines manually.
chop.suid <-
	function( sf ){

		# create a temporary file
		tf <- tempfile()
		
		# read the sas lines into memory
		sl <- readLines( sf )

		# figure out the position of the `suid` variable..
		where.to.chop <- which( grepl( 'suid' , tolower( sl ) ) & !grepl( 'ssuid' , tolower( sl ) ) )

		# if it exists..
		if( length( where.to.chop ) > 0 ){

			# find all semicolons in the document..
			semicolons <- grep( ';' , sl )

			# ..now, more precisely, find the first semicolon after the chop-line
			end.of.chop <- min( semicolons[ semicolons > where.to.chop ] ) - 1
			
			# remove non-public lines
			sl <- sl[ -where.to.chop:-end.of.chop ]

		}

		# write the sas import script to the text file..
		writeLines( sl , tf )

		# ..and return the position of the text file on the local disk.
		tf

	}
##################################################################################

