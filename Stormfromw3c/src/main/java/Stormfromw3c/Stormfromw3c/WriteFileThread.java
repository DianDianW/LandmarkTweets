package Stormfromw3c.Stormfromw3c;

public class WriteFileThread extends Thread{

    String StringJson = "";//
    boolean changedFlag = false;
    //int counter = 0;

    @Override
    public void run() {
        while(!interrupted()){
            while(changedFlag==true){
                //WriteJsonFile.WriteConfigJson(StringJson);
                //System.out.print(StringJson);
                this.changedFlag = false;
            }
        }

    }

    /*public void setStringJson(String jsonString){
        this.StringJson = jsonString;
        //StringJson+=jsonString;
        this.changedFlag = true;
    }*/
}