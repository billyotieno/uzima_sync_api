# UzimaSync: Employee Wellness - Project Tracking

## Phase 1: API Development (Processing Health Data)
- [x] Set up Flask project and environment
- [x] Create Flask app and necessary routes
- [x] Build API endpoint to receive and process health data from devices
- [x] Parse health data from incoming JSON payloads
- [x] Store processed health data into the **health_data** table in the Oracle database
- [x] Implement error handling for invalid data inputs
- [x] Write unit tests to ensure API stability and data integrity
- [x] Deploy Flask API to server (e.g., Oracle Cloud, AWS, etc.)

## Phase 2: Oracle Database Design & Integration
- [x] Define database schema for **health_data** table (fields: user_id, steps, calories, workout_duration, etc.)
- [x] Set up Oracle database connection with Flask API
- [x] Create migration scripts to initialize the Oracle database tables
- [x] Ensure proper indexing for efficient querying of user health data
- [x] Implement daily aggregation queries (in SQL) to process data into useful metrics (steps, calories, workout_duration)
- [x] Test Oracle database performance with bulk data ingestion

## Phase 3: Data Cleaning & Aggregation
- [x] Convert timestamps from incoming data to proper `datetime` format
- [x] Aggregate daily metrics (steps, calories, workout_duration) per user using SQL queries
- [x] Handle missing data or incomplete entries
- [ ] Write PLSQL procedures to calculate daily totals and update a **summary_metrics** table
- [ ] Test aggregation and calculation procedures for accuracy and consistency

## Phase 4: SQL/PLSQL Gamification Engine
- [ ] Define gamification rules (points for steps, calories, workout streaks) in SQL/PLSQL
- [ ] Write SQL/PLSQL functions to calculate points based on daily performance (steps, calories burned, workout streaks)
- [ ] Create a **user_points** table to store calculated points and badges
- [ ] Write PLSQL procedures to update points and badges based on defined gamification rules
- [ ] Implement triggers or scheduled jobs to update gamification data daily
- [ ] Test gamification engine with sample data

## Phase 5: Oracle APEX as a Progressive Web Application (PWA)
- [ ] Set up Oracle APEX application and enable PWA features (manifest file, service workers)
- [ ] Create interactive dashboard for employees to view daily steps, calories, and workout duration
- [ ] Build a leaderboard showing user rankings based on points
- [ ] Add gamification elements (badges, progress bars) to Oracle APEX dashboard
- [ ] Integrate Oracle APEX charts (e.g., bar charts, line charts) to visualize health metrics
- [ ] Add offline functionality and caching using service workers for the PWA
- [ ] Implement filtering and drill-down options for users to view historical performance
- [ ] Test Oracle APEX PWA application for responsiveness, performance, and user experience on multiple devices

## Phase 6: AI-Powered Health Recommendations (Using Oracle Generative AI Platform)
- [ ] Set up connection to Oracle Generative AI platform for personalized recommendations
- [ ] Define use-cases for AI-driven recommendations based on user health metrics (steps, calories, workout duration)
- [ ] Build integration to feed user data into Oracle Generative AI and retrieve personalized recommendations
- [ ] Display AI-generated recommendations in the Oracle APEX PWA dashboard
- [ ] Test the integration to ensure accuracy and relevance of AI recommendations
- [ ] Gather user feedback on the AI-driven recommendations to improve engagement

## Phase 7: Deployment and Testing
- [ ] Deploy Oracle APEX PWA, API, and Generative AI integration to production
- [ ] Set up continuous integration (CI) for automated testing of API, SQL procedures, and AI integration
- [ ] Conduct user acceptance testing (UAT) with real users
- [ ] Monitor performance and optimize as necessary
- [ ] Gather feedback from users and make iterative improvements

## Phase 8: User Engagement and Feedback
- [ ] Create onboarding documentation for new users in Oracle APEX PWA
- [ ] Set up feedback channels to gather insights from employees
- [ ] Implement improvements based on user feedback