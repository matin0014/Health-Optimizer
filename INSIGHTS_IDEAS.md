# Health Insights & Correlations Roadmap

This document outlines the custom insights we plan to generate. Unlike standard fitness apps that just show "what happened today," our goal is to find **correlations** and **trends** over time.

## 1. Sleep & Circadian Rhythm
*   **The "Consistency is King" Metric**
    *   **Logic:** Calculate the standard deviation of your *bedtime* and *wake-up time* over a rolling 14-day window.
    *   **Insight:** "Your sleep schedule has varied by less than 30 mins for the last 10 days. During this period, your Resting Heart Rate dropped by an average of 2 bpm."
    *   **Why it's better:** Fitbit tracks sleep duration, but rarely emphasizes the *regularity* of the timing, which is crucial for circadian health.

*   **"Social Jetlag" Detector**
    *   **Logic:** Compare average sleep midpoint on Weekdays vs. Weekends.
    *   **Insight:** "You shift your sleep schedule by +2 hours on weekends. This correlates with a 15% drop in 'Readiness' scores on Mondays."

*   **The "Late Night" Tax**
    *   **Logic:** Correlate *Bedtime Hour* with *Deep Sleep %*.
    *   **Insight:** "When you go to bed after 11:30 PM, your Deep Sleep decreases by an average of 15 minutes, regardless of how long you sleep."

## 2. Recovery & Strain (The "Athlete" View)
*   **Acute vs. Chronic Workload Ratio**
    *   **Logic:** Compare your activity (Steps/Zone Minutes) of the last 7 days (Acute) vs. the last 28 days (Chronic).
    *   **Insight:** "You are ramping up too fast. Your activity this week is 150% of your monthly average. Risk of injury or burnout is elevated."

*   **The "Lazy Day" Rebound**
    *   **Logic:** Look at days with < 3000 steps. Check the *next day's* RHR and HRV (if available).
    *   **Insight:** "Taking a complete rest day actually *increases* your stress metrics the next day. Active recovery (light walks) correlates with better recovery for you."

## 3. Metabolic & Nutrition (Future Integration)
*   **Meal Timing vs. Sleep Quality**
    *   **Logic:** (Requires Cronometer data) Time of last meal vs. Sleep Onset Latency (time to fall asleep) or HRV.
    *   **Insight:** "Eating within 2 hours of bed increases your average sleeping heart rate by 5 bpm."

*   **Sugar Hangover**
    *   **Logic:** High sugar intake days vs. Next day's "Energy" or Activity levels.
    *   **Insight:** "Days following high sugar intake show a 20% decrease in spontaneous physical activity (fidgeting/steps)."

## 4. Long-Term Health Audits
*   **Seasonal Affect Analysis**
    *   **Logic:** Compare activity and sleep duration in Winter vs. Summer.
    *   **Insight:** "You lose an average of 45 minutes of sleep per night in July compared to December."

*   **The "Stress" Spiral**
    *   **Logic:** Detect periods where RHR rises for 3+ consecutive days. Check correlation with Sleep Consistency.
    *   **Insight:** "Your RHR has been trending up for 4 days. In 80% of past cases, this pattern was preceded by inconsistent sleep times."
