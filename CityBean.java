import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mac on 2017/5/19.
 */
public class CityBean implements WritableComparable<CityBean> {
    private Long provienceId;
    private String cityName;
    private Long gdp;
    private Long population;

    public CityBean() {
    }

    public CityBean(Long provienceId, String cityName, Long gdp, Long population) {
        this.provienceId = provienceId;
        this.cityName = cityName;
        this.gdp = gdp;
        this.population = population;
    }

    public CityBean clone(){
        return new CityBean(provienceId, cityName, gdp, population);
    }

    public void readFields(DataInput dataInput) throws IOException {
        provienceId = dataInput.readLong();
        cityName = dataInput.readUTF();
        gdp = dataInput.readLong();
        population = dataInput.readLong();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(provienceId);
        dataOutput.writeUTF(cityName);
        dataOutput.writeLong(gdp);
        dataOutput.writeLong(population);
    }

    public Long getProvienceId() {
        return provienceId;
    }

    public String getProvienceName() {
        return cityName;
    }

    public Long getGDP() {
        return gdp;
    }

    public Long getPopulation() {
        return population;
    }

    @Override
    public String toString() {
        return provienceId + "\t" + cityName + "\t" + gdp + "\t" + population;
    }

    public int compareTo(CityBean o) {
        if (gdp > o.gdp){
            return 1;
        }
        else if (gdp < o.gdp){
            return -1;
        }
        else if( population > o.population){
            return 1;
        }
        else if (population < o.population){
            return -1;
        }
        else{
            return cityName.compareTo(o.cityName);
        }
    }
}