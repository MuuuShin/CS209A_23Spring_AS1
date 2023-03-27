package main.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.*;

public class OnlineCoursesAnalyzer {

    List<Course> courses = new ArrayList<>();

    Supplier<Stream<Course>> courseStreamSupplier;

    public OnlineCoursesAnalyzer(String datasetPath) {
        String line;
        //import data, use try-with-resources
        try (BufferedReader br = new BufferedReader(new FileReader(datasetPath, StandardCharsets.UTF_8))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] info = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
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

    //1
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

    //2
    public Map<String, Integer> getPtcpCountByInstAndSubject() {
        Stream<Course> courseStream = courseStreamSupplier.get();
        //sort by number of people and institution
//        Map<String, Integer> ptcpCountByInstAndSubject=
//                courseStream.map(x->new AbstractMap.SimpleEntry<>(x.getInstitution() +"-"+ x.getSubject(),x.getParticipants()))
//                        .sorted(Map.Entry::getKey)
        Map<String, Integer> ptcpCountByInstAndSubject =
                courseStream.collect(
                                Collectors.groupingBy(
                                        course -> course.getInstitution() + "-" + course.getSubject(),
                                        Collectors.summingInt(Course::getParticipants)))
                        .entrySet().stream().sorted((e1, e2) -> {
                            if (e1.getValue() == e2.getValue()) {
                                return e1.getKey().compareTo(e2.getKey());
                            } else {
                                return e2.getValue() - e1.getValue();
                            }
                        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o1, o2) -> o1, LinkedHashMap::new));
        return ptcpCountByInstAndSubject;
    }

    //3
    public Map<String, List<List<String>>> getCourseListOfInstructor() {
        Stream<Course> courseStream = courseStreamSupplier.get();
        Map<String, List<String>> singleCourseListOfInstructor =
                courseStream.filter(x -> !x.getInstructors().contains(","))
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
        for(String instructor:allInstructors){
            List<List<String>> courseList = new ArrayList<>();
            courseList.add(singleCourseListOfInstructor.getOrDefault(instructor, new ArrayList<>()));
            courseList.add(multiCourseListOfInstructor.getOrDefault(instructor, new ArrayList<>()));
            courseListOfInstructor.put(instructor, courseList);
        }
        return courseListOfInstructor;
    }

    //4
    public List<String> getCourses(int topK, String by) {
        return null;
    }

    //5
    public List<String> searchCourses(String courseSubject, double percentAudited, double totalCourseHours) {
        return null;
    }

    //6
    public List<String> recommendCourses(int age, int gender, int isBachelorOrHigher) {
        return null;
    }

}

class Course {
    String institution;
    String number;
    Date launchDate;
    String title;
    String instructors;
    String subject;
    int year;
    int honorCode;
    int participants;
    int audited;
    int certified;
    double percentAudited;
    double percentCertified;
    double percentCertified50;
    double percentVideo;
    double percentForum;
    double gradeHigherZero;
    double totalHours;
    double medianHoursCertification;
    double medianAge;
    double percentMale;
    double percentFemale;
    double percentDegree;

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
        if (title.startsWith("\"")) title = title.substring(1);
        if (title.endsWith("\"")) title = title.substring(0, title.length() - 1);
        this.title = title;
        if (instructors.startsWith("\"")) instructors = instructors.substring(1);
        if (instructors.endsWith("\"")) instructors = instructors.substring(0, instructors.length() - 1);
        this.instructors = instructors;
        if (subject.startsWith("\"")) subject = subject.substring(1);
        if (subject.endsWith("\"")) subject = subject.substring(0, subject.length() - 1);
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

    public String getInstitution() {
        return institution;
    }

    public String getNumber() {
        return number;
    }

    public Date getLaunchDate() {
        return launchDate;
    }

    public String getTitle() {
        return title;
    }

    public String getInstructors() {
        return instructors;
    }

    public String getSubject() {
        return subject;
    }

    public int getYear() {
        return year;
    }

    public int getHonorCode() {
        return honorCode;
    }

    public int getParticipants() {
        return participants;
    }

    public int getAudited() {
        return audited;
    }

    public int getCertified() {
        return certified;
    }

    public double getPercentAudited() {
        return percentAudited;
    }

    public double getPercentCertified() {
        return percentCertified;
    }

    public double getPercentCertified50() {
        return percentCertified50;
    }

    public double getPercentVideo() {
        return percentVideo;
    }

    public double getPercentForum() {
        return percentForum;
    }

    public double getGradeHigherZero() {
        return gradeHigherZero;
    }

    public double getTotalHours() {
        return totalHours;
    }

    public double getMedianHoursCertification() {
        return medianHoursCertification;
    }

    public double getMedianAge() {
        return medianAge;
    }

    public double getPercentMale() {
        return percentMale;
    }

    public double getPercentFemale() {
        return percentFemale;
    }

    public double getPercentDegree() {
        return percentDegree;
    }
}