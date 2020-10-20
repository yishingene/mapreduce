
import java.util.*;
// import java.lang.*;
// import java.util.Arrays;
// import java.util.List;
public class test {
    public static void main(String[] args){
      String str = "1,2,3,4";
      List<String> list=Arrays.asList(str.split(",")); // string 轉為 list
      System.out.println(list.get(0)); //取值
      System.out.println(Float.parseFloat(list.get(0))); //轉為 float
      System.out.println(Integer.parseInt(list.get(0))); //轉為 integer
      String str1 = "1234"; 
      float d1=0;
      int inum = Integer.parseInt(str1);//string 轉為 integer
      System.out.println(inum);

      //list 轉為 string
      List<String> list1=Arrays.asList("A","B","C");
      String delim=";";
      String res = String.join(delim,list1);
      System.out.println(res);
    
      String value = "1,2,3,4";
      String line = value.toString();
      List<String> list2=Arrays.asList(line.split(",")); 
      System.out.println(Float.parseFloat(list2.get(0)));




      for(int j = 1; j < 10; j++) { 
        for(int i = 2; i < 10; i++) { 
            System.out.printf("%d*%d=%2d ",i, j,  i * j);
        } 
        System.out.println(); 
     } 

     for(int j = 1; j < 10; j++) { 
        for(int i = 2; i < 10; i++) { 
            System.out.printf("%d*%d=%2d ",i, j,  i * j);
        } 
        System.out.println(); 
     } 

     for(int j = 0; j <= 10; j++) { 
        System.out.println(j);
     }

    //  float k = Math.min(2,2,5,1);
     System.out.println(Math.min(2.3,3.5));
    //   System.out.println("This is my first program in java");
      

    List<Float> comparenum=new ArrayList();
    comparenum.add(5.1f);
    comparenum.add(2.2f);
    comparenum.add(3.1f);
    System.out.println(comparenum);

    List<Float> list3=Arrays.asList(1.1f,2.3f,3.5f);
    System.out.println(list3);

    float i1=2;
    float i2=1;
    List<Float> list4=Arrays.asList(i1,i2);
    System.out.println(list4);

    Collections.sort(comparenum);
    System.out.println(comparenum);
    
    float a1=5;
    float a2=3;
    float a3=1;

    float index=1;
    float small = a1;

    if (a2<small){
      small=a2;
      index=2;
    }
    if (a3<small){
      small=a3;
      index=3;
    }
    System.out.println(small+"index is "+index);

    //float 轉 string
    float b1 = 0.3f;
    String str2 = Float.toString(b1);
    System.out.println(str2);


    }//End of main
  }//End of FirstJavaProgram Class