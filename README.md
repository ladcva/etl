# Running the Bash Script

To run the Bash script, follow these instructions:

### Step 1: Setup Virtual Environment

1. **Create a Virtual Environment:**

    ```bash
    virtualenv venv
    ```

    This command creates a virtual environment named `venv` in the current directory.

2. **Activate the Virtual Environment:**

    ```bash
    source venv/bin/activate
    ```

    This command activates the virtual environment. You should see `(venv)` appear at the beginning of your command prompt.

### Step 2: Install PySpark

3. **Install PySpark:**

    ```bash
    pip install -r requirements.txt
    ```

    This command installs all libraries and dependencies

### Step 3: Run the Python Module

4. **Run the Python Module:**

    ```bash
    python -m jobs.extract_employee
    ```

    Replace `jobs.extract_employee` with the correct module name and path of your Python script. This command executes the Python script that contains your job logic.

### Step 4: Deactivate Virtual Environment (Optional)

5. **Deactivate the Virtual Environment (Optional):**

    ```bash
    deactivate
    ```

    This command deactivates the virtual environment and returns you to your normal shell environment.

### Additional Notes

- Make sure you have Java installed on your system, as PySpark requires it to run.
- Verify that your Python script (`jobs.extract_employee`) contains the necessary code for your job logic.
- You may need to configure additional settings or provide input data paths depending on your specific project requirements.

Once you have completed these steps, the Bash script should run successfully.