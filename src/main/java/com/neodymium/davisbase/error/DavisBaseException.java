<<<<<<< HEAD
package com.neodymium.davisbase.error;

public final class DavisBaseException extends RuntimeException {
    public DavisBaseException(String message) {
        super(message);
    }
}
=======
package com.neodymium.davisbase.error;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DavisBaseException extends RuntimeException {
    public DavisBaseException(Throwable throwable) {
        super(throwable);
        log.error("Exception occurred while executing the program: ", throwable);
    }

    public DavisBaseException(String message) {
        super(message);
        log.error("Exception occurred while executing the program: {}", message);
    }
}
>>>>>>> 5689cd5b61352fa8d05eec945a1947c15e453842
