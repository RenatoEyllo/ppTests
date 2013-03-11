package com.eyllo.paprika.test.exception;

/**
 * Custom exception class for the Glacier Service
 */
public class PaprikaStressException extends RuntimeException {


  /**
   * Default serial version number 
   */
  private static final long serialVersionUID = 1L;

  /**
   * Constructor for the GlacierException class
   * @param message
   */
  public PaprikaStressException(String message) {
    super(message);
  }
}
