package mrpolyonymous;

import java.time.OffsetDateTime;

/**
 * Simple representation of a <code>[min, max)</code> half-open interval
 */
record TimeInterval(OffsetDateTime min, OffsetDateTime max) {

}
