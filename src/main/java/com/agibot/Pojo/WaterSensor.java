package com.agibot.Pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor implements Serializable {
    private String id;
    private Long ts;
    private Integer vc;
}
