package main.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.*;


/**
 * The type Online courses analyzer.
 */
public class OnlineCoursesAnalyzer {

  /**
   * The Courses.
   */
  List<Course> courses = new ArrayList<>();

  /**
   * The Course stream supplier.
   */
  Supplier<Stream<Course>> courseStreamSupplier;

  /**
   * Instantiates a new Online courses analyzer.
   *
   * @param datasetPath the dataset path
   */
  public OnlineCoursesAnalyzer(String datasetPath) {
    String line;
    //import data, use try-with-resources
    try (BufferedReader br =
                 new BufferedReader(new FileReader(datasetPath, StandardCharsets.UTF_8))) {
      br.readLine();
      while ((line = br.readLine()) != null) {
        String[] info = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        Course course = new Course(info[0], info[1], new Date(info[2]), info[3], info[4], info[5],
                Integer.parseInt(info[6]), Integer.parseInt(info[7]), Integer.parseInt(info[8]),
                Integer.parseInt(info[9]), Integer.parseInt(info[10]), Double.parseDouble(info[11]),
                Double.parseDouble(info[12]), Double.parseDouble(info[13]), Double.parseDouble(info[14]),
                Double.parseDouble(info[15]), Double.parseDouble(info[16]), Double.parseDouble(info[17]),
                Double.parseDouble(info[18]), Double.parseDouble(info[19]), Double.parseDouble(info[20]),
                Double.parseDouble(info[21]), Double.parseDouble(info[22]));
        courses.add(course);
      }
      courseStreamSupplier = () -> courses.stream();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Gets ptcp count by inst.
   *
   * @return the ptcp count by inst
   */
  public Map<String, Integer> getPtcpCountByInst() {
    Stream<Course> courseStream = courseStreamSupplier.get();
    Map<String, Integer> ptcpCountByInst =
            courseStream.collect(
                    Collectors.groupingBy(
                            Course::getInstitution,
                            Collectors.summingInt(Course::getParticipants)
                    ));
    return ptcpCountByInst;
  }

  /**
   * Gets ptcp count by inst and subject.
   *
   * @return the ptcp count by inst and subject
   */
  public Map<String, Integer> getPtcpCountByInstAndSubject() {
    Stream<Course> courseStream = courseStreamSupplier.get();
    //sort by number of people and institution
    //        Map<String, Integer> ptcpCountByInstAndSubject=
    //                courseStream.map(x->new AbstractMap.SimpleEntry<>
    //                (x.getInstitution() +"-"+ x.getSubject(),x.getParticipants()))
    //                .sorted(Map.Entry::getKey)
    Map<String, Integer> ptcpCountByInstAndSubject = courseStream
            .collect(
                    Collectors.groupingBy(
                            course -> course.getInstitution() + "-" + course.getSubject(),
                            Collectors.summingInt(Course::getParticipants)))
            .entrySet().stream().sorted((e1, e2) -> {
              if (Objects.equals(e1.getValue(), e2.getValue())) {
                return e1.getKey().compareTo(e2.getKey());
              } else {
                return e2.getValue() - e1.getValue();
              }
            }).collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (o1, o2) -> o1,
                    LinkedHashMap::new
            ));
    return ptcpCountByInstAndSubject;
  }

  /**
   * Gets course list of instructor.
   *
   * @return the course list of instructor
   */
  public Map<String, List<List<String>>> getCourseListOfInstructor() {
    Stream<Course> courseStream = courseStreamSupplier.get();
    Map<String, List<String>> singleCourseListOfInstructor =
            courseStream.filter(x -> !x.getInstructors().contains(","))
                    .sorted(Comparator.comparing(Course::getTitle))
                    .map(x -> new AbstractMap.SimpleEntry<>(x.getInstructors(), x.getTitle()))
                    .distinct()
                    .collect(
                            Collectors.groupingBy(
                                    Map.Entry::getKey,
                                    Collectors.mapping(Map.Entry::getValue, Collectors.toList())
                            ));
    courseStream = courseStreamSupplier.get();
    Map<String, List<String>> multiCourseListOfInstructor =
            courseStream.filter(x -> x.getInstructors().contains(", "))
                    .sorted(Comparator.comparing(Course::getTitle))
                    .flatMap(x -> {
                      String[] instructors = x.getInstructors().split(", ");
                      return Arrays.stream(instructors).map(y -> new AbstractMap.SimpleEntry<>(y, x.getTitle()));
                    })
                    .distinct()
                    .collect(
                            Collectors.groupingBy(
                                    Map.Entry::getKey,
                                    Collectors.mapping(Map.Entry::getValue, Collectors.toList())
                            ));
    //find all instructors
    Set<String> allInstructors = new HashSet<>();
    allInstructors.addAll(singleCourseListOfInstructor.keySet());
    allInstructors.addAll(multiCourseListOfInstructor.keySet());
    //merge two maps
    Map<String, List<List<String>>> courseListOfInstructor = new HashMap<>();
    for (String instructor : allInstructors) {
      List<List<String>> courseList = new ArrayList<>();
      courseList.add(singleCourseListOfInstructor.getOrDefault(instructor, new ArrayList<>()));
      courseList.add(multiCourseListOfInstructor.getOrDefault(instructor, new ArrayList<>()));
      courseListOfInstructor.put(instructor, courseList);
    }
    return courseListOfInstructor;
  }

  /**
   * Gets courses.
   *
   * @param topK the top k
   * @param by   the by
   * @return the courses
   */
  public List<String> getCourses(int topK, String by) {
    Stream<Course> courseStream = courseStreamSupplier.get();
    List<String> topCourses;
    switch (by) {
      case "hours" -> topCourses = courseStream
              .collect(Collectors.toMap(Course::getTitle, Course::getTotalHours, Math::max))
              .entrySet().stream()
              .sorted(
                      Comparator.comparing((Function<Map.Entry<String, Double>, Double>) Map.Entry::getValue)
                              .reversed().thenComparing(Map.Entry::getKey)
              )
              .limit(topK).map(Map.Entry::getKey).toList();
      case "participants" -> topCourses = courseStream
              .collect(Collectors.toMap(Course::getTitle, Course::getParticipants, Math::max))
              .entrySet().stream()
              .sorted(
                      Comparator.comparing((Function<Map.Entry<String, Integer>, Integer>) Map.Entry::getValue)
                              .reversed().thenComparing(Map.Entry::getKey)
              )
              .limit(topK).map(Map.Entry::getKey).toList();
      default -> topCourses = new ArrayList<>();
    }
    return topCourses;
  }

  /**
   * Search courses list.
   *
   * @param courseSubject    the course subject
   * @param percentAudited   the percent audited
   * @param totalCourseHours the total course hours
   * @return the list
   */
  public List<String> searchCourses(String courseSubject, double percentAudited, double totalCourseHours) {
    String courseSubjectLow = courseSubject.toLowerCase();
    Stream<Course> courseStream = courseStreamSupplier.get();
    List<String> searchCourses = courseStream
            .filter(x -> Pattern.matches(".*" + courseSubjectLow + ".*", x.getSubject().toLowerCase()))
            .filter(x -> x.getPercentAudited() >= percentAudited)
            .filter(x -> x.getTotalHours() <= totalCourseHours)
            .map(Course::getTitle).sorted().distinct().toList();
    return searchCourses;
  }

  /**
   * Recommend courses list.
   *
   * @param age                the age
   * @param gender             the gender
   * @param isBachelorOrHigher the is bachelor or higher
   * @return the list
   */
  public List<String> recommendCourses(int age, int gender, int isBachelorOrHigher) {
    Stream<Course> courseStream = courseStreamSupplier.get();

    Map<String, CourseScore> courseScoreList = courseStream.collect(
            Collectors.groupingBy(
                    Course::getNumber,
                    Collectors.collectingAndThen(
                            Collectors.toList(),
                            list -> {
                              double ageSum = 0, genderSum = 0, degreeSum = 0;
                              int ageCnt = 0, genderCnt = 0, degreeCnt = 0, year = -1;
                              String courseTitle = "";
                              for (Course course : list) {
                                if (course.getYear() > year) {
                                  courseTitle = course.getTitle();
                                  year = course.getYear();
                                }
                                ageSum += course.getMedianAge();
                                genderSum += course.getPercentMale();
                                degreeSum += course.getPercentDegree();
                                ageCnt++;
                                genderCnt++;
                                degreeCnt++;
                              }
                              double ageAvg = ageSum / ageCnt;
                              double genderAvg = genderSum / genderCnt;
                              double degreeAvg = degreeSum / degreeCnt;
                              return new CourseScore(courseTitle, ageAvg, genderAvg, degreeAvg);
                            }
                    )
            )
    );

    List<String> recommendCourses = courseScoreList.entrySet().stream().sorted((o1, o2) -> {
      double c1 = o1.getValue().calcScore(age, gender, isBachelorOrHigher);
      double c2 = o2.getValue().calcScore(age, gender, isBachelorOrHigher);
      if (c1 == c2) {
        return o1.getValue().courseTitle.compareTo(o2.getValue().courseTitle);
      } else {
        return Double.compare(c1, c2);
      }
    }).map(x -> x.getValue().courseTitle).distinct().limit(10).toList();

    return recommendCourses;
  }
}


/**
 * The type Course score.
 */
class CourseScore {

  /**
   * The Age avg.
   */
  double ageAvg;
  /**
   * The Gender avg.
   */
  double genderAvg;
  /**
   * The Degree avg.
   */
  double degreeAvg;
  /**
   * The Course title.
   */
  String courseTitle;

  /**
   * Instantiates a new Course score.
   *
   * @param courseTitle the course title
   * @param ageAvg      the age avg
   * @param genderAvg   the gender avg
   * @param degreeAvg   the degree avg
   */
  public CourseScore(String courseTitle, double ageAvg, double genderAvg, double degreeAvg) {
    this.courseTitle = courseTitle;
    this.ageAvg = ageAvg;
    this.genderAvg = genderAvg;
    this.degreeAvg = degreeAvg;
  }

  /**
   * Calc score double.
   *
   * @param age                the age
   * @param gender             the gender
   * @param isBachelorOrHigher the is bachelor or higher
   * @return the double
   */
  public double calcScore(double age, double gender, double isBachelorOrHigher) {
    return Math.pow(age - ageAvg, 2)
            + Math.pow(gender * 100 - genderAvg, 2)
            + Math.pow(isBachelorOrHigher * 100 - degreeAvg, 2);
  }
}


/**
 * The type Course.
 */
class Course {
  /**
   * The Institution.
   */
  String institution;
  /**
   * The Number.
   */
  String number;
  /**
   * The Launch date.
   */
  Date launchDate;
  /**
   * The Title.
   */
  String title;
  /**
   * The Instructors.
   */
  String instructors;
  /**
   * The Subject.
   */
  String subject;
  /**
   * The Year.
   */
  int year;
  /**
   * The Honor code.
   */
  int honorCode;
  /**
   * The Participants.
   */
  int participants;
  /**
   * The Audited.
   */
  int audited;
  /**
   * The Certified.
   */
  int certified;
  /**
   * The Percent audited.
   */
  double percentAudited;
  /**
   * The Percent certified.
   */
  double percentCertified;
  /**
   * The Percent certified 50.
   */
  double percentCertified50;
  /**
   * The Percent video.
   */
  double percentVideo;
  /**
   * The Percent forum.
   */
  double percentForum;
  /**
   * The Grade higher zero.
   */
  double gradeHigherZero;
  /**
   * The Total hours.
   */
  double totalHours;
  /**
   * The Median hours certification.
   */
  double medianHoursCertification;
  /**
   * The Median age.
   */
  double medianAge;
  /**
   * The Percent male.
   */
  double percentMale;
  /**
   * The Percent female.
   */
  double percentFemale;
  /**
   * The Percent degree.
   */
  double percentDegree;

  /**
   * Instantiates a new Course.
   *
   * @param institution              the institution
   * @param number                   the number
   * @param launchDate               the launch date
   * @param title                    the title
   * @param instructors              the instructors
   * @param subject                  the subject
   * @param year                     the year
   * @param honorCode                the honor code
   * @param participants             the participants
   * @param audited                  the audited
   * @param certified                the certified
   * @param percentAudited           the percent audited
   * @param percentCertified         the percent certified
   * @param percentCertified50       the percent certified 50
   * @param percentVideo             the percent video
   * @param percentForum             the percent forum
   * @param gradeHigherZero          the grade higher zero
   * @param totalHours               the total hours
   * @param medianHoursCertification the median hours certification
   * @param medianAge                the median age
   * @param percentMale              the percent male
   * @param percentFemale            the percent female
   * @param percentDegree            the percent degree
   */
  public Course(String institution, String number, Date launchDate,
                String title, String instructors, String subject,
                int year, int honorCode, int participants,
                int audited, int certified, double percentAudited,
                double percentCertified, double percentCertified50,
                double percentVideo, double percentForum, double gradeHigherZero,
                double totalHours, double medianHoursCertification,
                double medianAge, double percentMale, double percentFemale,
                double percentDegree) {
    this.institution = institution;
    this.number = number;
    this.launchDate = launchDate;
    if (title.startsWith("\"")) {
      title = title.substring(1);
    }
    if (title.endsWith("\"")) {
      title = title.substring(0, title.length() - 1);
    }
    this.title = title;
    if (instructors.startsWith("\"")) {
      instructors = instructors.substring(1);
    }
    if (instructors.endsWith("\"")) {
      instructors = instructors.substring(0, instructors.length() - 1);
    }
    this.instructors = instructors;
    if (subject.startsWith("\"")) {
      subject = subject.substring(1);
    }
    if (subject.endsWith("\"")) {
      subject = subject.substring(0, subject.length() - 1);
    }
    this.subject = subject;
    this.year = year;
    this.honorCode = honorCode;
    this.participants = participants;
    this.audited = audited;
    this.certified = certified;
    this.percentAudited = percentAudited;
    this.percentCertified = percentCertified;
    this.percentCertified50 = percentCertified50;
    this.percentVideo = percentVideo;
    this.percentForum = percentForum;
    this.gradeHigherZero = gradeHigherZero;
    this.totalHours = totalHours;
    this.medianHoursCertification = medianHoursCertification;
    this.medianAge = medianAge;
    this.percentMale = percentMale;
    this.percentFemale = percentFemale;
    this.percentDegree = percentDegree;
  }

  /**
   * Gets institution.
   *
   * @return the institution
   */
  public String getInstitution() {
    return institution;
  }

  /**
   * Gets number.
   *
   * @return the number
   */
  public String getNumber() {
    return number;
  }

  /**
   * Gets title.
   *
   * @return the title
   */
  public String getTitle() {
    return title;
  }

  /**
   * Gets instructors.
   *
   * @return the instructors
   */
  public String getInstructors() {
    return instructors;
  }

  /**
   * Gets subject.
   *
   * @return the subject
   */
  public String getSubject() {
    return subject;
  }

  /**
   * Gets year.
   *
   * @return the year
   */
  public int getYear() {
    return year;
  }

  /**
   * Gets participants.
   *
   * @return the participants
   */
  public int getParticipants() {
    return participants;
  }

  /**
   * Gets percent audited.
   *
   * @return the percent audited
   */
  public double getPercentAudited() {
    return percentAudited;
  }

  /**
   * Gets total hours.
   *
   * @return the total hours
   */
  public double getTotalHours() {
    return totalHours;
  }


  /**
   * Gets median age.
   *
   * @return the median age
   */
  public double getMedianAge() {
    return medianAge;
  }

  /**
   * Gets percent male.
   *
   * @return the percent male
   */
  public double getPercentMale() {
    return percentMale;
  }

  /**
   * Gets percent degree.
   *
   * @return the percent degree
   */
  public double getPercentDegree() {
    return percentDegree;
  }

}