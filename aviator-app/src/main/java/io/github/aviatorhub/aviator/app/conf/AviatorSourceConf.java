package io.github.aviatorhub.aviator.app.conf;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import io.github.aviatorhub.aviator.app.constant.ConsumeMode;

@Getter
@Setter
@NoArgsConstructor
public class AviatorSourceConf {
  @Parameter(names = {"-cm", "-consume-mode"},
      description = "the consume mode for kafka, default is GROUP_OFFSETS, "
          + "you can choose EARLIEST, LATEST, GROUP_OFFSETS, TIMESTAMP")
  private ConsumeMode consumeMode;

  @Parameter(names = {"-ct", "-consume-timestamp"},
  description = "the consume timestamp for kafka with TIMESTAMP consume mode."
      + " the default value is 86400, which means a day ago.")
  private Long timestamp;
}
