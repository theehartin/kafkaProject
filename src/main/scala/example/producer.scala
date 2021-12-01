//spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 ./project3_2.11-0.1.0-SNAPSHOT.jar

package example

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class MyProducer {

  /**
    * topic = test_topic
    * sleepTime is in milliseconds
    * terminate with CTRL + c
    * 
    */



  //Lists/////////////////////////////////////////////////////////////////

  //100 First Names
  val first_nameList = List(
    "Olivia",
    "Emma",
    "Ava",
    "Charlotte",
    "Sophia",
    "Amelia",
    "Isabella",
    "Mia",
    "Evelyn",
    "Harper",
    "Camila",
    "Gianna",
    "Abigail",
    "Luna",
    "Ella",
    "ELizabeth",
    "Sofia",
    "Emily",
    "Avery",
    "Mila",
    "Scarlett",
    "Eleanor",
    "Madison",
    "Layla",
    "Penelope",
    "Aria",
    "Chloe",
    "Grace",
    "Ellie",
    "Nora",
    "Hazel",
    "Zoey",
    "Riley",
    "Victoria",
    "Lily",
    "Aurora",
    "Violet",
    "Nova",
    "Hannah",
    "Emilia",
    "Zoe",
    "Stella",
    "Everly",
    "Isla",
    "Leah",
    "Lillian",
    "Addison",
    "Willow",
    "Lucy",
    "Paisley",
    "Liam",
    "Noah",
    "Oliver",
    "Elijah",
    "William",
    "James",
    "Benjamin",
    "Lucas",
    "Henry",
    "Alexander",
    "Mason",
    "Michael",
    "Ethan",
    "Daniel",
    "Jacob",
    "Logan",
    "Jackson",
    "Levi",
    "Sebastian",
    "Mateo",
    "Jack",
    "Owen",
    "Theodore",
    "Aiden",
    "Samuel",
    "Joseph",
    "John",
    "David",
    "Wyatt",
    "Matthew",
    "Luke",
    "Asher",
    "Carter",
    "Julian",
    "Grayson",
    "Grayson",
    "Leo",
    "Jayden",
    "Gabriel",
    "Isaac",
    "Lincoln",
    "Anthony",
    "Hudson",
    "Dylan",
    "Ezra",
    "Thomas",
    "Charles",
    "Christopher",
    "Jaxson",
    "Maverick",
    "Josiah"
  )

  //100 Last Names
  val last_nameList = List(
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzales",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "FLores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
    "Roberts",
    "Gomez",
    "Phillips",
    "Evans",
    "Turner",
    "Diaz",
    "Parker",
    "Cruz",
    "Edwards",
    "Collins",
    "Reyes",
    "Stewart",
    "Morris",
    "Morales",
    "Murphy",
    "Cook",
    "Rogers",
    "Gutierrez",
    "Ortiz",
    "Morgan",
    "Cooper",
    "Peterson",
    "Bailey",
    "Reed",
    "Kelly",
    "Howard",
    "Ramos",
    "Kim",
    "Cox",
    "Ward",
    "Richardson",
    "Watson",
    "Brooks",
    "Chavez",
    "Wood",
    "James",
    "Bennet",
    "Gray",
    "Mendoza",
    "Ruiz",
    "Hughes",
    "Price",
    "Alvarez",
    "Castillo",
    "Sanders",
    "Patel",
    "Myers",
    "Long",
    "Ross",
    "Foster",
    "Jimenez",
    "Hennig"
  )

  //100 Universities
  val universityList = List(
    "Butternut University", 
    "Shasta University", 
    "Reindahl University", 
    "Granby University", 
    "Di Loreto University", 
    "Cody University", 
    "Doe Crossing University", 
    "Donald University", 
    "Crescent Oaks University", 
    "Cody  University", 
    "Magdeline University", 
    "Elmside University", 
    "Spaight University", 
    "Eliot University", 
    "Colorado University", 
    "Tennessee University", 
    "Sommers University", 
    "Paget University", 
    "Birchwood University", 
    "Mccormick University", 
    "Waubesa University", 
    "Talmadge University", 
    "Hanson University", 
    "Warner University", 
    "Tennyson University", 
    "Oakridge University", 
    "Amoth University", 
    "Corry  University", 
    "Heffernan  University", 
    "Scoville University", 
    "Lakewood Gardens University", 
    "Charing Cross University", 
    "Mitchell University", 
    "Fordem University", 
    "Vidon University", 
    "Talisman University", 
    "Acker University", 
    "Spaight University", 
    "Sunbrook University", 
    "Myrtle University", 
    "Hanson University", 
    "Wayridge University", 
    "Sommers University", 
    "Petterle University", 
    "Lawn University", 
    "Tennessee University", 
    "Corben University", 
    "Northview University", 
    "Comanche University", 
    "Spenser University", 
    "West University", 
    "Arapahoe University", 
    "Farragut University", 
    "Burrows University", 
    "Graedel University", 
    "Glacier Hill University", 
    "Menomonie University", 
    "Reinke University", 
    "Ridgeway University", 
    "Browning University", 
    "Di Loreto University", 
    "Lukken University", 
    "Maple Wood University", 
    "Fremont University", 
    "Parkside University", 
    "Lighthouse Bay University", 
    "Nancy University", 
    "Starling University", 
    "Elliot University", 
    "Sunfield University", 
    "Oak Valley University", 
    "Hallows University", 
    "International University", 
    "American Ash University", 
    "Shoshone University", 
    "Del Mar University", 
    "Farmco University", 
    "Stuart University", 
    "Warner University", 
    "Melody University", 
    "Corscot University", 
    "Mccormick University", 
    "Independence University", 
    "Hansons University", 
    "Esker University", 
    "John Wall University", 
    "Farmco University", 
    "Harvard University", 
    "Spenser University", 
    "Shelley University", 
    "Clarendon University", 
    "Dorton University", 
    "Russell University", 
    "Waxwing University", 
    "Pennsylvania University", 
    "Lien University", 
    "Lawn University", 
    "Meadow Vale University", 
    "Tony University", 
    "Elka University"
  )

  //296 Majors
  val majorList = List(
    "Agriculture, General",
    "Agribusiness Operations",
    "Agricultural Business & Management",
    "Agricultural Economics",
    "Agricultural Mechanization",
    "Agricultural Production",
    "Agronomy & Crop Science",
    "Animal Sciences",
    "Food Sciences & Technology",
    "Horticulture Operations & Management",
    "Horticulture Science",
    "Natural Resources Conservation, General",
    "Environmental Science",
    "Forestry",
    "Natural Resources Management",
    "Wildlife & Wildlands Management",
    "ARCHITECTURE",
    "Architecture, General",
    "Architectural Environmental Design",
    "City/Urban/Regional Planning",
    "Interior Architecture",
    "Landscape Architecture",
    "Area Studies, General (e.g., African, Middle Eastern)",
    "Asian Area Studies",
    "European Area Studies",
    "Latin American Area Studies",
    "North American Area Studies",
    "Ethnic & Minority Studies, General",
    "African American Studies",
    "American Indian/Native American Studies",
    "Latino/Chicano Studies",
    "Women’s Studies",
    "Liberal Arts & General Studies*",
    "Library Science",
    "Multi/Interdisciplinary Studies*",
    "Art, General",
    "Art History, Criticism & Conservation",
    "Fine/Studio Arts",
    "Cinema/Film",
    "Cinematography/Film/Vide Production",
    "Dance",
    "Design & Visual Communications, General",
    "Fashion/Apparel Design",
    "Graphic Design",
    "Industrial Design",
    "Interior Design",
    "Music, General",
    "Music, Performance",
    "Music, Theory & Composition",
    "Photography",
    "Theatre Arts/Drama",
    "Accounting",
    "Accounting Technician",
    "Business Administration & Management, General",
    "Hotel/Motel Management",
    "Human Resources Development/Training",
    "Human Resources Management",
    "International Business Management",
    "Labor/Industrial Relations",
    "Logistics & Materials Management",
    "Marketing Management & Research",
    "Office Supervision & Management",
    "Operations Management & Supervision",
    "Organizational Behavior",
    "Purchasing/Procurement/Contracts Management",
    "Restaurant/Food Services Management",
    "Small Business Management/Operations",
    "Travel/Tourism Management",
    "Business/Management Quantitative Methods, General",
    "Actuarial Science*",
    "Business/Managerial Economics",
    "Finance, General",
    "Banking & Financial Support Services",
    "Financial Planning & Services",
    "Insurance & Risk Management",
    "Investments & Securities",
    "Management Information Systems",
    "Real Estate",
    "Sales, Merchandising, & Marketing, General",
    "Fashion Merchandising",
    "Tourism & Travel Marketing",
    "Secretarial Studies & Office Administration",
    "Communications, General",
    "Advertising",
    "Digital Communications/Media",
    "Journalism, Broadcast",
    "Journalism, Print",
    "Mass Communications",
    "Public Relations & Organizational Communication",
    "Radio & Television Broadcasting",
    "Communications Technology, General",
    "Graphic & Printing Equipment Operation*",
    "Multimedia/Animation/Special Effects",
    "Radio & Television Broadcasting Technology*",
    "COMMUNITY, FAMILY, & PERSONAL SERVICES",
    "Family & Consumer Sciences, General",
    "Adult Development & Aging/Gerontology",
    "Child Care Services Management",
    "Child Development",
    "Consumer & Family Economics",
    "Food & Nutrition",
    "Textile & Apparel",
    "Parks, Recreation, & Leisure, General",
    "Exercise Science/Physiology/Kinesiology",
    "Health & Physical Education/Fitness",
    "Parks/Rec/Leisure Facilities Management",
    "Sport & Fitness Administration/Management",
    "Personal Services, General*",
    "Cosmetology/Hairstyling*",
    "Culinary Arts/Chef Training",
    "Funeral Services & Mortuary Science",
    "Protective Services, General",
    "Corrections",
    "Criminal Justice",
    "Fire Protection & Safety Technology",
    "Law Enforcement",
    "Military Technologies*",
    "Public Administration & Services, General",
    "Community Organization & Advocacy",
    "Public Administration",
    "Public Affairs & Public Policy Analysis",
    "Social Work",
    "Computer & Information Sciences, General",
    "Computer Networking/Telecommunications",
    "Computer Science & Programming",
    "Computer Software & Media Applications",
    "Computer System Administration",
    "Data Management Technology",
    "Information Science",
    "Webpage Design",
    "Mathematics, General",
    "Applied Mathematics",
    "Statistics",
    "Counseling & Student Services",
    "Educational Administration",
    "Special Education",
    "Teacher Education, General",
    "Curriculum & Instruction",
    "Early Childhood Education",
    "Elementary Education",
    "Junior High/Middle School Education",
    "Postsecondary Education",
    "Secondary Education",
    "Teacher Assisting/Aide Education",
    "Teacher Education, Subject-Specific*",
    "Agricultural Education",
    "Art Education",
    "Business Education",
    "Career & Technical Education",
    "English-as-a-Second-Language Education",
    "English/Language Arts Education",
    "Foreign Languages Education",
    "Health Education",
    "Mathematics Education",
    "Music Education",
    "Physical Education & Coaching",
    "Science Education",
    "Social Studies/Sciences Education",
    "Engineering (Pre-Engineering), General",
    "Aerospace/Aeronautical Engineering",
    "Agricultural/Bioengineering",
    "Architectural Engineering",
    "Biomedical Engineering",
    "Chemical Engineering",
    "Civil Engineering",
    "Computer Engineering",
    "Construction Engineering/Management",
    "Electrical, Electronics & Communications Engineering",
    "Environmental Health Engineering",
    "Industrial Engineering",
    "Mechanical Engineering",
    "Nuclear Engineering",
    "Drafting/CAD Technology, General",
    "Architectural Drafting/CAD Technology",
    "Mechanical Drafting/CAD Technology",
    "Engineering Technology, General",
    "Aeronautical/Aerospace Engineering Technologies",
    "Architectural Engineering Technology",
    "Automotive Engineering Technology",
    "Civil Engineering Technology",
    "Computer Engineering Technology",
    "Construction/Building Technology",
    "Electrical, Electronics Engineering Technologies",
    "Electromechanical/Biomedical Engineering Technologies",
    "Environmental Control Technologies",
    "Industrial Production Technologies",
    "Mechanical Engineering Technology",
    "Quality Control & Safety Technologies",
    "Surveying Technology",
    "English Language & Literature, General",
    "American/English Literature",
    "Creative Writing",
    "Public Speaking",
    "Foreign Languages/Literatures, General",
    "Asian Languages & Literatures",
    "Classical/Ancient Languages & Literatures",
    "Comparative Literature",
    "French Language & Literature",
    "German Language & Literature",
    "Linguistics",
    "Middle Eastern Languages & Literatures",
    "Spanish Language & Literature",
    "Health Services Administration,General",
    "Hospital/Facilities Administration",
    "Medical Office/Secretarial",
    "Medical Records",
    "Medical/Clinical Assisting, General",
    "Dental Assisting",
    "Medical Assisting",
    "Occupational Therapy Assisting",
    "Physical Therapy Assisting",
    "Veterinarian Assisting/Technology",
    "Chiropractic (Pre-Chiropractic)",
    "Dental Hygiene",
    "Dentistry (Pre-Dentistry)",
    "Emergency Medical Technology",
    "Health-Related Professions & Services, General*",
    "Athletic Training",
    "Communication Disorder Services (e.g., Speech Pathology)",
    "Public Health",
    "Health/Medical Technology, General",
    "Medical Laboratory Technology",
    "Medical Radiologic Technology",
    "Nuclear Medicine Technology",
    "Respiratory Therapy Technology",
    "Surgical Technology",
    "Medicine (Pre-Medicine)",
    "Nursing, Practical/Vocational (LPN)",
    "Nursing, Registered (BS/RN)",
    "Optometry (Pre-Optometry)",
    "Osteopathic Medicine",
    "Pharmacy (Pre-Pharmacy)",
    "Physician Assisting",
    "Therapy & Rehabilitation, General",
    "Alcohol/Drug Abuse Counseling",
    "Massage Therapy",
    "Mental Health Counseling",
    "Occupational Therapy",
    "Physical Therapy (Pre-Physical Therapy)",
    "Psychiatric/Mental Health Technician",
    "Rehabilitation Therapy",
    "Vocational Rehabilitation Counseling",
    "Veterinary Medicine (Pre-Veterinarian)",
    "Philosophy",
    "Religion",
    "Theology, General",
    "Bible/Biblical Studies",
    "Divinity/Ministry",
    "Religious Education",
    "Aviation & Airway Science, General",
    "Aircraft Piloting & Navigation",
    "Aviation Management & Operations",
    "Construction Trades (e.g., carpentry, plumbing, electrical)",
    "Mechanics & Repairers, General",
    "Aircraft Mechanics/Technology",
    "Autobody Repair/Technology",
    "Automotive Mechanics/Technology",
    "Avionics Technology",
    "Diesel Mechanics/Technology",
    "Electrical/Electronics Equip Installation & Repair",
    "Heating/Air Cond/Refrig Install/Repair",
    "Precision Production Trades, General",
    "Machine Tool Technology",
    "Welding Technology",
    "Transportation & Materials Moving (e.g., air, ground, & marine)",
    "Biology, General",
    "Biochemistry & Biophysics",
    "Cell/Cellular Biology",
    "Ecology",
    "Genetics",
    "Marine/Aquatic Biology",
    "Microbiology & Immunology",
    "Zoology",
    "Physical Sciences, General",
    "Astronomy",
    "Atmospheric Sciences & Meteorology",
    "Chemistry",
    "Geological & Earth Sciences",
    "Physics",
    "Legal Studies, General*",
    "Court Reporting*",
    "Law (Pre-Law)",
    "Legal Administrative Assisting/Secretarial*",
    "Paralegal/Legal Assistant",
    "Social Sciences, General",
    "Anthropology",
    "Criminology",
    "Economics",
    "Geography",
    "History",
    "International Relations & Affairs",
    "Political Science & Government",
    "Psychology, Clinical & Counseling",
    "Psychology, General",
    "Sociology",
    "Urban Studies/Urban Affairs"
  )
  
  //50 States
  val stateList = List(
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming"
  )

  //4 Types
  val screenTypeList = List (
    "Spark",
    "Standard",
    "Business",
    "Analyst"
  )

  val minute = List("00","15","30","45")
  val pastNoonOrNotList = List("AM", "PM")
  val contactMethodList = List("Phone","Email","SMS")
  val response = List("Accept","Reject","Delay")

  ///////////////////////////////////////////////////////////////////////////


  val r = new scala.util.Random

  def valueDeterminer(field: String): ProducerRecord[String, String] = {

    val topic = "test_topic"
    val key = "SRK" //Some Random Key
    val id = r.nextInt(1000)
    val screenerid = r.nextInt(1000)
    val recruiterid = r.nextInt(1000)
    val qualifiedLeadid = r.nextInt(1000)
    val firstName = first_nameList(r.nextInt(100))//100
    val lastName = last_nameList(r.nextInt(100))//100
    val emailHandle = "@gmail.com"
    val day = r.nextInt(30)+1
    val month = r.nextInt(12)
    val year = (2022)
    val hour = r.nextInt(12)
    val university = universityList(r.nextInt(100))//100
    val major = majorList(r.nextInt(296))//296
    val email = firstName+lastName+emailHandle
    val home_state = stateList(r.nextInt(50))//50
    val pastNoonOrNot = pastNoonOrNotList(r.nextInt(2))
    val date = (month.toString()
    +"/"+day.toString()
    +"/"+year.toString())
    val start_time = (hour.toString
    +":"+minute(r.nextInt(4))
    +" "+pastNoonOrNot)
    val end_time = ((hour+1).toString
    +":"+minute(r.nextInt(4))
    +" "+pastNoonOrNot)
    val contactMethod = contactMethodList(r.nextInt(3))//3
    val screenType = screenTypeList(r.nextInt(4))//4
    val offerActionDate = (month.toString()
    +"/"+(day+r.nextInt(10)).toString()
    +"/"+year.toString())//They reply sometime within 10 days
    val offerAction = response(r.nextInt(3))
    var value = ""



    if(field == "Screener" | field == "Recruiter"){
     value = (
     field+","+
     id +","+
     firstName+","+
     lastName) 
    }//End of 'if(field == "Screener|field == "Recruiter"")' 
      

    if(field == "Qualified Lead"){
      value =(
      field+","+
      id +","+
      firstName+","+
      lastName+","+
      university+","+
      major+","+
      email+","+
      home_state)
    }//End of 'if(field == "Qualified Lead")'

    if(field == "Contact Attempts"){
      value = (
      field+","+
      recruiterid+","+
      qualifiedLeadid+","+
      date+","+
      start_time+","+
      end_time+","+
      contactMethod)
    }//End of 'if(field == "Contact Attempts")'


    if(field == "Screening"){
      value = (
      field+","+
      screenerid+","+
      qualifiedLeadid+","+
      date+","+
      start_time+","+
      end_time+","+
      screenType+","+
      r.nextInt(10).toString+","+//Number of questions
      r.nextInt(10).toString) //Number of accepted 
    }//End of 'if(field == "Screening")'


    if(field == "Offers"){
      value = (
      field+","+
      screenerid+","+
      recruiterid+","+
      qualifiedLeadid+","+
      date+","+
      offerActionDate+","+
      contactMethod+","+
      offerAction)
    }//End of 'if(field == "Offers")'

    val record = new ProducerRecord[String, String](
      topic,
      key,
      value
    )
    return record
  }//End of valueDeterminer()

  

  def produce(){


    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("firstProducerProgram")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    

    val props: Properties = new Properties()
    //props.put("bootstrap.servers","localhost:9092")
    props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    // The acks config controls the criteria under which requests are considered complete.
    // The "all" setting we have specified will result in blocking on the full commit of the record,
    // the slowest but most durable setting.
    props.put("acks", "all")
    val producer = new KafkaProducer[String, String](props)





    val topic = "test_topic"
    val key = "SRK" //Some Random Key
    
    try {
      println("I AM THE PRODUCER")
      var i = 0
      val sleepTime = scala.io.StdIn.readLine("sleepTime: ").toInt
    while(true){
      //6 Fields
      val fieldList = List(
        "Screener","Recruiter","Qualified Lead",
        "Contact Attempts", "Screening", "Offers"
      )
      val field = fieldList(r.nextInt(6))  
      val record = valueDeterminer(field)

      producer.send(record)
      println(record.key(), record.value)
 
      println(i)
      Thread.sleep(sleepTime)
      i+=1
    }//End of while

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }//End of 'finally'
  }//End of produce()
}//End of Class
